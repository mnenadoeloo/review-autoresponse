from typing import TypeVar, Callable, Awaitable, Optional, Any
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    RetryError,
    before_sleep_log,
)
import logging
from app.core.config import settings
from openai import APIError, APITimeoutError, APIConnectionError, APIStatusError

logger = logging.getLogger(__name__)

T = TypeVar('T')

def create_retry_decorator(
    max_retries: Optional[int] = None,
    retry_codes: Optional[set[int]] = None,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Create a retry decorator with custom settings."""
    
    max_retries = max_retries or settings.LLM_MAX_RETRIES
    retry_codes = retry_codes or settings.RETRY_STATUS_CODES

    def should_retry_exception(exception: Exception) -> bool:
        """Determine if the exception should trigger a retry."""
        if isinstance(exception, APIStatusError):
            return exception.status_code in retry_codes
        return isinstance(exception, (APIError, APITimeoutError, APIConnectionError))

    return retry(
        stop=stop_after_attempt(max_retries),
        wait=wait_exponential(
            multiplier=settings.LLM_RETRY_DELAY,
            max=settings.LLM_RETRY_MAX_DELAY
        ),
        retry=retry_if_exception_type(should_retry_exception),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True
    )

async def with_retry(
    func: Callable[..., Awaitable[T]],
    *args: Any,
    max_retries: Optional[int] = None,
    retry_codes: Optional[set[int]] = None,
    **kwargs: Any,
) -> T:
    """Execute an async function with retry logic."""
    retry_decorator = create_retry_decorator(max_retries, retry_codes)
    
    @retry_decorator
    async def wrapped_func():
        return await func(*args, **kwargs)
    
    try:
        return await wrapped_func()
    except RetryError as e:
        logger.error(f"All retry attempts failed for {func.__name__}: {str(e)}")
        raise 