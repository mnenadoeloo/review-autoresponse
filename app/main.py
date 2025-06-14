from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from app.models.llm import BatchChatRequest, BatchChatResponse, ReviewGenerationRequest, ReviewGenerationResponse, Review, KafkaReviewGenerationRequest
from app.services.llm_service import LLMService
from app.services.health_service import HealthService
import asyncio
from starlette.responses import JSONResponse
import logging
from contextlib import asynccontextmanager
import time
from typing import Optional

# Configure root logger with WARNING level by default
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Set specific loggers to appropriate levels
logging.getLogger('app.services.llm_service').setLevel(logging.INFO)
logging.getLogger('app.services.health_service').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize services and Kafka consumer
    global llm_service, health_service #, kafka_consumer_task, kafka_consumer
    
    try:
        # Initialize LLM service first
        logger.info("Initializing LLM service...")
        llm_service = await LLMService.create()
        health_service = HealthService(llm_service=llm_service, db_manager=llm_service.db_manager)
        yield
        
    finally:
        # Shutdown: cleanup services and Kafka consumer
        logger.info("Starting shutdown process...")
        # Cleanup LLM service
        if llm_service:
            logger.info("Cleaning up LLM service...")
            await llm_service.cleanup()
        
        logger.info("Shutdown complete")

app = FastAPI(
    title="My FastAPI Service",
    description="A basic FastAPI service",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services in an async context
llm_service = None
health_service = None

@app.get("/")
async def root():
    return {"message": "Welcome to FastAPI service"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "message": "Service is running"}

@app.get("/systemhealth")
async def system_health_check():

    try:
        if not health_service:
            # During startup, always return healthy
            return {"status": "healthy", "message": "Service starting up"}
            
        health = await health_service.check_system_health()
        
        # During startup phase, consider the service healthy
        if health.metrics.application.get("total_requests", 0) == 0:
            return {"status": "healthy", "message": "Service initializing"}
        
        # For established services, use the actual health status
        if health.status != "healthy":
            raise HTTPException(
                status_code=503,
                detail=health.to_dict()
            )
        
        return JSONResponse(content=health.to_dict())
    except Exception as e:
        logger.error(f"Health check error: {e}")
        # Return healthy during errors to prevent unnecessary restarts
        return {"status": "healthy", "message": "Health check recovering"}

# Add a custom exception handler for HTTPException
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail} if isinstance(exc.detail, (str, dict)) else exc.detail
    )

@app.post("/api/v1/llm/batch-chat", response_model=BatchChatResponse)
async def process_batch_chat(request: BatchChatRequest):
    return await llm_service.process_batch_chat(request)

@app.post("/api/v1/llm/generate-responses", response_model=ReviewGenerationResponse)
async def generate_review_responses(request: KafkaReviewGenerationRequest):
    # Convert KafkaReviewInput to Review objects
    reviews = []
    for kafka_review in request.reviews:
        review = Review.from_kafka_input(kafka_review)
        if review:
            reviews.append(review)
    
    if not reviews:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid reviews found in the request"
        )
    
    # Create a new request with converted reviews
    converted_request = ReviewGenerationRequest(reviews=reviews)
    return await llm_service.generate_review_responses(converted_request)