from openai import AsyncOpenAI
from typing import List, Dict, Any, Optional, Tuple
from app.core.config import settings
from app.models.llm import (
    BatchChatRequest, 
    BatchChatResponse, 
    ChatResponse, 
    Message,
    ProcessedReview,
    ChatPromptRequest,
    ReviewGenerationRequest,
    ReviewGenerationResponse,
    GenerationResponse,
    NmIdData,
    Review,
)
from app.services.postgresql import PostgresRepository
from app.services.db_service import DatabaseManager
from app.services.statika import StatikaRepository
from app.services.greenplum import GPConnection 
import json
import logging
# Импортирование промптов, которые выглядят по примеру из app.prompts
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import asyncio
import aiohttp
import math
import requests
import re
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import partial
from app.core.performance_settings import performance_settings
from cachetools import TTLCache
import functools
from app.core.circuit_breaker import CircuitBreaker
from contextlib import asynccontextmanager
import time
from datetime import datetime
from asyncio import Semaphore
from app.utils.retry_handler import with_retry
from openai import APIError, APIStatusError
from app.utils.message_queue import MessageQueue
import httpx
import gc
import psutil
import os
import pandas as pd

# Configure logger
logger = logging.getLogger(__name__)

class LLMService:
    def __init__(self):
        """Regular initialization for attributes that don't need async setup."""
        self.start_time = datetime.now()
        self.metrics = {
            "requests_per_minute": 0.0,
            "average_response_time": 0.0,
            "successful_requests": 0,
            "total_requests": 0,
            "failed_requests": 0,
            "last_update": self.start_time
        }
        self.category_prompts = {
            "...": ...,
            "...": ...,
            "...": ...,
            "...": ...,
        }
        self.assistant_prompts = {
            "...": ...,
            "...": ...,
            "...": ...,
            "...": ...,
        }
        self.prompt_template = """Товар: {item_name}; Категория: {category}; Рейтинг: {rating}; Отзыв: {review} - Ответ:"""
        self.default_system_prompt = SYSTEM_PROMPT
        self.default_assistant_prompt = ASSISTANT_TEMPLATE

        # Reduce background queue size from 1000 to 200
        self.background_queue = asyncio.Queue(maxsize=1000)
        self.rate_limiter = asyncio.Semaphore(10)  # Limit concurrent requests
        self.max_concurrent_requests = 10
        self.memory_threshold = 3500  # 3.5GB threshold
        
    async def __aenter__(self):
        """Async context manager entry."""
        await self._initialize_resources()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.cleanup()
        
    @classmethod
    async def create(cls):
        """Async factory method to create and initialize LLMService instance."""
        self = cls()
        await self._initialize_resources()
        return self
        
    async def _initialize_resources(self):
        """Initialize all async resources."""
        # Initialize HTTP client
        self.http_client = httpx.AsyncClient(
            limits=httpx.Limits(max_keepalive_connections=100, max_connections=100),
            timeout=httpx.Timeout(timeout=settings.LLM_REQUEST_TIMEOUT)
        )
        
        # Initialize OpenAI client
        self.client = AsyncOpenAI(
            api_key=settings.OPENAI_API_KEY,
            base_url=settings.OPENAI_BASE_URL,
            http_client=self.http_client
        )
        
        # Initialize database manager
        self.statika = StatikaRepository()
        self.db_manager = DatabaseManager()
        self.gp_manager = GPConnection()
        self.postgres_repo = PostgresRepository()
        await self.postgres_repo.init_pool()
        logger.info("PostgreSQL repository initialized")
        
        # Initialize process and thread pools
        self.process_pool = ProcessPoolExecutor(
            max_workers=performance_settings.PROCESS_POOL_SIZE
        )
        self.thread_pool = ThreadPoolExecutor(
            max_workers=performance_settings.THREAD_POOL_SIZE
        )
        
        # Initialize concurrency controls
        self.request_timeout = performance_settings.REQUEST_TIMEOUT
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        self.batch_semaphore = asyncio.Semaphore(performance_settings.MAX_BATCH_CONCURRENT)
        self._request_semaphore = asyncio.Semaphore(settings.LLM_MAX_CONCURRENT_REQUESTS)
        
        # Initialize caches
        self.prompt_cache = TTLCache(maxsize=100, ttl=120)  # 2 minutes TTL
        self.classification_cache = TTLCache(maxsize=100, ttl=120)  # 2 minutes TTL
        
        # Initialize queues and circuit breakers
        self.message_queue = MessageQueue()
        
        self.db_circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            reset_timeout=60,
            half_open_timeout=30
        )
        self.api_circuit_breaker = CircuitBreaker(
            failure_threshold=3,
            reset_timeout=30,
            half_open_timeout=15
        )

        # Initialize api for getting reviews
        self.headers = {
            "Authorization": settings.OPENAI_API_KEY,
            "X-Service-Name": settings.X_SERVICE_NAME,
            "X-Service-Token": settings.X_SERVICE_TOKEN
        }
  
        # Start background tasks
        self.metrics_task = asyncio.create_task(self._update_metrics())
        self.background_task = asyncio.create_task(self._process_background_tasks())
        self.retry_task = asyncio.create_task(self._process_retry_queue())
        self.gc_task = asyncio.create_task(self._periodic_gc())
        
    async def _periodic_gc(self):
        """Periodically run garbage collection and log memory usage."""
        while True:
            try:
                memory_before = self._get_memory_usage()
                gc.collect()
                memory_after = self._get_memory_usage()
                
                logger.info(
                    f"Periodic GC completed - Memory before: {memory_before:.2f} MB, "
                    f"after: {memory_after:.2f} MB, freed: {max(0, memory_before - memory_after):.2f} MB"
                )
                
                # Sleep for 30 seconds before next collection (reduced from 60)
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                logger.info("Garbage collection task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in garbage collection: {str(e)}")
                await asyncio.sleep(5)  # Sleep briefly on error before retrying

    async def cleanup(self):
        """Cleanup resources."""
        logger.info("Starting LLMService cleanup...")
        
        # Cancel all background tasks
        tasks_to_cancel = []
        for task_name in ['metrics_task', 'background_task', 'retry_task', 'gc_task']:
            if hasattr(self, task_name):
                task = getattr(self, task_name)
                if not task.done():
                    logger.debug(f"Cancelling {task_name}...")
                    task.cancel()
                    tasks_to_cancel.append(task)
        
        # Wait for all tasks to be cancelled
        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        
        # Wait for background queue to empty with timeout
        if hasattr(self, 'background_queue'):
            try:
                logger.debug("Waiting for background queue to empty...")
                await asyncio.wait_for(self.background_queue.join(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for background queue to empty")
            except Exception as e:
                logger.error(f"Error waiting for background queue: {e}")
        
        # Cleanup executors
        for pool_name, pool_attr in [('process_pool', 'process_pool'), ('thread_pool', 'thread_pool')]:
            if hasattr(self, pool_attr):
                logger.debug(f"Shutting down {pool_name}...")
                getattr(self, pool_attr).shutdown(wait=True)
        
        # Cleanup HTTP client
        if hasattr(self, 'http_client'):
            logger.debug("Closing httpx client...")
            await self.http_client.aclose()
        
        logger.info("LLMService cleanup completed")

    async def _process_background_tasks(self):
        """Process background tasks like logging and analytics."""
        while True:
            try:
                task = await self.background_queue.get()
                try:
                    await task
                except Exception as e:
                    logger.error(f"Background task error: {str(e)}")
                finally:
                    self.background_queue.task_done()
            except asyncio.CancelledError:
                # Handle cancellation gracefully
                logger.info("Background task processor cancelled")
                break
            except Exception as e:
                logger.error(f"Background task processor error: {str(e)}")
                # Don't call task_done() here since we didn't get a task

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, Exception))
    )
    async def _process_single_chat(self, messages: List[Message], index: int) -> ChatResponse:
        """Enhanced chat processing with circuit breaker and rate limiting."""
        start_time = time.time()
        
        try:
            async with self._rate_limit():
                # Properly await the circuit breaker
                completion = await self.api_circuit_breaker(
                    self.client.chat.completions.create
                )(
                    model=settings.OPENAI_MODEL_NAME,
                    messages=[{"role": msg.role, "content": msg.content} for msg in messages],
                    max_tokens=settings.MAX_TOKENS,
                    temperature=settings.TEMPERATURE,
                    timeout=self.request_timeout
                )
                
                # Update metrics
                self.metrics["successful_requests"] += 1
                self.metrics["average_response_time"] = (
                    (self.metrics["average_response_time"] * (self.metrics["total_requests"] - 1) +
                    (time.time() - start_time)) / self.metrics["total_requests"]
                )
                
                return ChatResponse(
                    content=completion.choices[0].message.content,
                    conversation_index=index
                )
                
        except Exception as e:
            self.metrics["failed_requests"] += 1
            logger.error(f"Error processing chat {index}: {str(e)}")
            raise

    async def process_batch_chat(self, request: BatchChatRequest) -> BatchChatResponse:
        """Process batch chat requests concurrently without chunking for higher throughput."""
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        tasks = []

        async def process_with_semaphore(conv, idx):
            async with semaphore:
                return await self._process_single_chat(conv.messages, idx)

        for i, conv in enumerate(request.conversations):
            tasks.append(asyncio.create_task(process_with_semaphore(conv, i)))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        responses = []
        for i, resp in enumerate(results):
            if isinstance(resp, Exception):
                logger.error(f"Error in processing conversation {i}: {str(resp)}")
                responses.append(ChatResponse(
                    content="I apologize, but I encountered an error processing this request.",
                    conversation_index=i
                ))
            else:
                responses.append(resp)
        return BatchChatResponse(responses=responses)

    def _get_memory_usage(self):
        """Get current memory usage of the process."""
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024  # Convert to MB

    async def _process_reviews_batch(self, reviews: List[Review]) -> List[GenerationResponse]:
        memory_before = self._get_memory_usage()
        logger.info(f"Memory usage before batch processing: {memory_before:.2f} MB")
        
        async with self.batch_semaphore:
            try:
                # Pre-fetch all required data with error handling
                product_data = await self._fetch_product_data_batch({r.nm_id for r in reviews})
                
                # Only process reviews where we successfully got product data
                valid_reviews = [r for r in reviews if r.nm_id in product_data]
                
                if not valid_reviews:
                    logger.error("No valid product data found for any reviews in batch")
                    return []
                
                async def process_single_review(review):
                    try:
                        async with self._rate_limit():
                            return await self._generate_single_response(
                                review,
                                product_data[review.nm_id]
                            )
                    except Exception as e:
                        logger.error(f"Error processing review for nm_id {review.nm_id}: {str(e)}")
                        return None
                
                results = await asyncio.gather(
                    *[process_single_review(review) for review in valid_reviews]
                )
                
                gc.collect()
                
                memory_after = self._get_memory_usage()
                logger.info(f"Memory usage after batch processing: {memory_after:.2f} MB (Δ: {memory_after - memory_before:.2f} MB)")
                
                return [r for r in results if r is not None]
                
            except Exception as e:
                logger.error(f"Batch processing error: {str(e)}")
                raise

    async def _fetch_product_data_batch(self, nm_ids: set[int]) -> Dict[int, NmIdData]:
        """Fetch product data for multiple nm_ids efficiently with proper error handling."""
        results = {}
        
        async def fetch_with_retry(nm_id: int) -> Optional[NmIdData]:
            try:
                return await self.db_manager.get_nm_id_data(nm_id)
            except Exception as e:
                logger.error(f"Error fetching data for nm_id {nm_id}: {str(e)}")
                return None
        
        # Use gather instead of TaskGroup to avoid coroutine reuse issues
        tasks = [fetch_with_retry(nm_id) for nm_id in nm_ids]
        results_list = await asyncio.gather(*tasks, return_exceptions=False)
        
        return {
            nm_id: result 
            for nm_id, result in zip(nm_ids, results_list)
            if result is not None
        }

    async def generate_review_responses(
        self,
        request: ReviewGenerationRequest
    ) -> ReviewGenerationResponse:
        """Generate responses for reviews with validation and error recovery."""
        try:
            memory_usage = self._get_memory_usage()
            if memory_usage > self.memory_threshold:
                logger.warning(f"Memory usage too high ({memory_usage:.2f} MB). Triggering GC.")
                gc.collect()
                memory_after_gc = self._get_memory_usage()
                if memory_after_gc > self.memory_threshold:
                    raise RuntimeError(f"Memory usage too high ({memory_after_gc:.2f} MB) to process request")

            async with self.rate_limiter:
                validated_reviews = [
                    Review(**self._validate_review_data(review.dict()))
                    for review in request.reviews
                ]
                
                batch_size = performance_settings.MAX_BATCH_SIZE
                total_reviews = len(validated_reviews)
                generations: List[GenerationResponse] = []
                
                for i in range(0, total_reviews, batch_size):
                    batch = validated_reviews[i:i + batch_size]
                    
                    try:
                        batch_results = await self._process_reviews_batch(batch)
                        generations.extend(batch_results)
                        
                        self.metrics["total_requests"] += len(batch)
                        self.metrics["successful_requests"] += len(batch_results)
                        
                    except Exception as e:
                        logger.error(f"Batch processing error: {str(e)}")
                        self.metrics["failed_requests"] += len(batch)
                        continue
                
                return ReviewGenerationResponse(generations=generations)
                
        except Exception as e:
            logger.error(f"Review generation error: {str(e)}")
            raise

    async def _log_batch_metrics(self, batch_size: int):
        """Log batch processing metrics (runs in background)."""
        # Add your metrics logging logic here
        pass

    @asynccontextmanager
    async def _rate_limit(self):
        """Rate limiter context manager with timeout."""
        try:
            async with asyncio.timeout(performance_settings.REQUEST_TIMEOUT):
                async with self.rate_limiter:
                    yield
        except asyncio.TimeoutError:
            logger.error("Rate limit timeout exceeded")
            raise

    async def _update_metrics(self):
        """Update service metrics periodically."""
        while True:
            try:
                # Calculate requests per minute
                self.metrics["requests_per_minute"] = (
                    self.metrics["total_requests"] / 
                    (datetime.now() - self.start_time).total_seconds() * 60
                )
                
                # Log metrics
                logger.info(f"Service metrics: {self.metrics}")
                
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Error updating metrics: {e}")
                await asyncio.sleep(5)

    def _validate_review_data(self, review: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and sanitize review data."""
        validated = review.copy()
        
        # Ensure rating is valid
        rating = validated.get('rating', 1)
        if not isinstance(rating, int) or rating < 1 or rating > 5:
            validated['rating'] = 1
            logger.warning(f"Invalid rating {rating} for review {validated.get('id_review')}, defaulting to 1")
        
        # Ensure nm_id is valid
        if isinstance(validated.get('nm_id'), str):
            try:
                validated['nm_id'] = int(validated['nm_id'])
            except (ValueError, TypeError):
                logger.warning(f"Invalid nm_id for review {validated.get('id_review')}")
        
        return validated

    def get_related_product(self, nm_id: int):
        with self.gp_manager as conn:
            related_product = conn.get_first_rec_item(nm_id)
            return related_product

    async def get_documents_from_api(self, nm_id: int) -> List[Dict]:
        url = "..."
        payload = {
            "nmId": nm_id,
            "skip": 0,
            "take": 30
        }

        async with httpx.AsyncClient(verify=False) as client:
            response = await client.post(url, json=payload, headers=self.headers)
            feedbacks = response.json().get("feedbacks", [])

        data = []
        for fb in feedbacks:
            answer_text = fb.get("...", {}).get("...") if fb.get("...") else None
            product_details = fb.get("...", {})
            data.append({
                "...": fb.get("..."),
                "...": fb.get("..."),
                "...": fb.get("..."),
                "...": fb.get("..."),
                "...": fb.get("..."),
                "...": fb.get("..."),
                "...": fb.get("..."),
                "...": fb.get("..."),
                "...": fb.get("..."),
            })

        df = pd.DataFrame(data)

        documents = [
            {
                "doc_id": i,
                "title": row["..."],
                "productValuation": row["..."],
                "content": f"Отзыв: {row['...']}\n Ответ на отзыв:{row['...']}"
            }
            for i, row in df.iterrows()
        ]
        return documents

    async def run_statika(self, nm_id: int):
        async with aiohttp.ClientSession() as session:
            card = await self.statika.get_card_from_statika(nm_id, session)
            return card
    
    async def get_options_by_nmid(self, nm_id: int):
        url = f"..."
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    print(f"Ошибка при запросе: {response.status}")
                    return None
                
    def replace_single_article(self, text: str, related_product: str):
        if related_product and text:
            return re.sub(r"\(арт\. [^)]+\)", f"(арт. {related_product})", text, count=1)
        return text

    async def _generate_single_response(
        self,
        review: Review,
        product_data: NmIdData
    ) -> GenerationResponse:
        """Generate response for a single review."""
        try:
            ASSISTANT_TEMPLATE = self.assistant_prompts.get(
                product_data.category,
                self.default_assistant_prompt
            )

            documents = await self.get_documents_from_api(review.nm_id)
            card_info = await self.run_statika(review.nm_id)
            
            related_product = self.get_related_product(review.nm_id)

            relatedProducts = "Сопутствующих товаров не найдено."
            product_name = None
            
            if related_product:
                try:
                    nm_data = await self.get_options_by_nmid(related_product)
                    
                    # Используем get с цепочкой для безопасного доступа
                    selling_data = nm_data.get("...") if nm_data else None
                    supplier_id = selling_data.get("...") if selling_data else None
                    product_name = nm_data.get("...") if nm_data else None
                    
                    if selling_data and product_name:
                        relatedProducts = f"Сопутствующий товар: {product_name} (арт. {related_product})"
                    else:
                        logger.warning(f"Incomplete data for nm_id {related_product}. selling_data: {bool(selling_data)}, product_name: {bool(product_name)}")
                        
                except Exception as e:
                    logger.error(f"Error getting options for nm_id {related_product}: {str(e)}")

            logger.info(f"Related products from GP table: {relatedProducts}")

            ASSISTANT_PROMPT = ASSISTANT_TEMPLATE.format(cardInfo=card_info)

            PROMPT = self.prompt_template.format(
                item_name=product_data.title,
                category=product_data.category,
                rating=review.rating,
                review=review.review
            )
            
            DOCUMENT_PROMPT = DOCUMENTS_PROMPT.format(documents=json.dumps(documents, ensure_ascii=False))
            
            messages = [
                {"role": "system", "content": GROUNDED_SYSTEM_PROMPT},
                {"role": "assistant", "content": ASSISTANT_PROMPT},
                {"role": "documents", "content": DOCUMENT_PROMPT},
                {"role": "user", "content": PROMPT}
            ]
            
            if related_product:
                RELATED_PRODUCT_PROMPT = RELATED_PRODUCTS_PROMPT.format(relatedProducts=relatedProducts)
                # Добавляем сообщение о связанном товаре только если он существует
                messages.insert(3, {"role": "assistant", "content": RELATED_PRODUCT_PROMPT})
            
            # Generate response using OpenAI
            output = await self.client.chat.completions.create(
                    model=settings.OPENAI_MODEL_NAME,
                    messages=messages,
                    max_tokens=settings.MAX_TOKENS,
                    temperature=settings.TEMPERATURE
                )
            
            # Create metadata for the response
            metadata = ProcessedReview(
                review=review,
                product_data=product_data,
            )

            output = output.choices[0].message.content[output.choices[0].message.content.find("!")+2:]
            output = output[:output.find(" С уважением")]

            greeting = "Здравствуйте! "
            response = greeting + output.replace("nm_id:", "арт.")
            logger.info(f"Response before replace article: {response}")
            
            try:
                response = self.replace_single_article(response, related_product)
            except Exception as e:
                logger.error(f"Error replacing article in response: {str(e)}")

            log_data = {
                "...": "...",
                "...": "...",
                "...": "...",
                "...": "...",
                "...": "...",
                "...": "..."
            }
            
            try:
                # Пробуем основной метод
                await self.postgres_repo.insert_to_postgres(log_data)
            except Exception as e:
                logger.warning(f"Primary logging method failed: {str(e)}")
                try:
                    # Если основной метод не сработал, пробуем альтернативный
                    logger.info("Trying alternative PostgreSQL logging method...")
                    await self.postgres_repo.insert_to_postgres_alternative(log_data)
                except Exception as alt_e:
                    # Если и альтернативный метод не сработал, логируем ошибку
                    logger.error(f"Both logging methods failed. Alternative method error: {str(alt_e)}")

            return GenerationResponse(
                response=response,
                metadata=metadata
            )

        except Exception as e:
            logger.error(f"Error generating response for review {review.id_review}: {str(e)}")
            raise

    @asynccontextmanager
    async def _request_context(self):
        """Context manager for handling LLM requests with concurrency control."""
        async with self._request_semaphore:
            try:
                yield
            except APIStatusError as e:
                if e.status_code in settings.RETRY_STATUS_CODES:
                    logger.warning(f"Retryable error occurred: {str(e)}")
                raise
            except APIError as e:
                logger.error(f"API error occurred: {str(e)}")
                raise

    async def _make_llm_request(self, messages: List[Message], **kwargs) -> str:
        """Make an LLM request with retry logic."""
        async with self._request_context():
            response = await with_retry(
                self.client.chat.completions.create,
                messages=[m.dict() for m in messages],
                model=settings.OPENAI_MODEL_NAME,
                max_tokens=kwargs.get('max_tokens', settings.MAX_TOKENS),
                temperature=kwargs.get('temperature', settings.TEMPERATURE)
            )
            return response.choices[0].message.content

    async def _process_retry_queue(self):
        """Background task to process retry queue."""
        while True:
            try:
                ready_messages = await self.message_queue.get_ready_messages()
                for message in ready_messages:
                    try:
                        # Attempt to process the message
                        response = await self._make_llm_request(
                            message.payload["messages"],
                            **message.payload.get("kwargs", {})
                        )
                        await self.message_queue.mark_success(message.id)
                    except Exception as e:
                        logger.error(f"Retry failed for message {message.id}: {str(e)}")
                        await self.message_queue.mark_failure(message.id)
                
                # Sleep before next check
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error processing retry queue: {str(e)}")
                await asyncio.sleep(5)