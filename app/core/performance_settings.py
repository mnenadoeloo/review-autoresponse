from pydantic_settings import BaseSettings
from typing import Optional

class PerformanceSettings(BaseSettings):
    """Performance-related settings."""
    
    # Concurrency settings
    MAX_CONCURRENT_REQUESTS: int = 50
    MAX_BATCH_SIZE: int = 15
    MAX_BATCH_CONCURRENT: int = 10
    REQUEST_TIMEOUT: int = 30
    
    # Pool sizes
    PROCESS_POOL_SIZE: int = 4
    THREAD_POOL_SIZE: int = 8
    DB_POOL_SIZE: int = 20
    
    # Cache settings
    CACHE_TTL: int = 3600
    CACHE_MAX_SIZE: int = 1000
    
    # Rate limiting
    RATE_LIMIT_PER_MINUTE: int = 600
    RATE_LIMIT_BURST: int = 100
    
    class Config:
        env_prefix = "PERFORMANCE_"

performance_settings = PerformanceSettings() 