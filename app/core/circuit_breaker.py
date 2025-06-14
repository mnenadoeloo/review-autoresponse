from datetime import datetime, timedelta
from typing import Optional, Callable, Any
import asyncio
from functools import wraps
import logging

logger = logging.getLogger(__name__)

class CircuitBreaker:
    """Circuit breaker pattern implementation."""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        reset_timeout: int = 60,
        half_open_timeout: int = 30,
        min_throughput: int = 10
    ):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.half_open_timeout = half_open_timeout
        self.min_throughput = min_throughput
        
        self.failures = 0
        self.state = "closed"
        self.last_failure_time: Optional[datetime] = None
        self.success_count = 0
        self.total_requests = 0
        
        # Metrics
        self.metrics = {
            "total_failures": 0,
            "total_successes": 0,
            "last_failure": None,
            "last_success": None,
            "current_state": "closed"
        }
    
    async def before_call(self):
        """Check circuit state before making a call."""
        if self.state == "open":
            if not self.last_failure_time:
                raise CircuitBreakerError("Circuit is OPEN")
                
            if (datetime.now() - self.last_failure_time).seconds >= self.reset_timeout:
                self.state = "half-open"
                self.metrics["current_state"] = "half-open"
            else:
                raise CircuitBreakerError("Circuit is OPEN")
                
        elif self.state == "half-open" and self.success_count >= self.min_throughput:
            self.state = "closed"
            self.failures = 0
            self.success_count = 0
            self.metrics["current_state"] = "closed"
    
    async def on_success(self):
        """Handle successful calls."""
        self.success_count += 1
        self.total_requests += 1
        self.metrics["total_successes"] += 1
        self.metrics["last_success"] = datetime.now()
        
        if self.state == "half-open" and self.success_count >= self.min_throughput:
            self.state = "closed"
            self.failures = 0
            self.metrics["current_state"] = "closed"
    
    async def on_failure(self):
        """Handle failed calls."""
        self.failures += 1
        self.total_requests += 1
        self.metrics["total_failures"] += 1
        self.last_failure_time = datetime.now()
        self.metrics["last_failure"] = self.last_failure_time
        
        if self.failures >= self.failure_threshold:
            self.state = "open"
            self.metrics["current_state"] = "open"
    
    def __call__(self, func):
        """Decorator implementation."""
        @wraps(func)
        async def wrapper(*args, **kwargs):
            await self.before_call()
            
            try:
                result = await func(*args, **kwargs)
                await self.on_success()
                return result
            except Exception as e:
                await self.on_failure()
                raise
                
        return wrapper

class CircuitBreakerError(Exception):
    """Custom exception for circuit breaker errors."""
    pass 