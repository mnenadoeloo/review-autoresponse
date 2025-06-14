from pydantic_settings import BaseSettings
from pydantic import validator

class Settings(BaseSettings):
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "My FastAPI Service"
    
    # OpenAI settings
    OPENAI_API_KEY: str  # Will be provided by Vault
    OPENAI_BASE_URL: str = # Will be provided by Vault
    OPENAI_MODEL_NAME: str = # Will be provided by Vault
    MAX_TOKENS: int = # Will be provided by Vault
    TEMPERATURE: float = # Will be provided by Vault
    
    # Footwear Database settings
    FOOTWEAR_DB_NAME: str = # Will be provided by Vault
    FOOTWEAR_DB_USER: str = # Will be provided by Vault
    FOOTWEAR_DB_PASSWORD: str # Will be provided by Vault
    FOOTWEAR_DB_HOST: str = # Will be provided by Vault
    FOOTWEAR_DB_PORT: str = # Will be provided by Vault
    FOOTWEAR_DB_SSL: str = # Will be provided by Vault

    # Greenplum settings
    GP_DATABASE: str # Will be provided by Vault
    GP_USER: str # Will be provided by Vault
    GP_PASSWORD: str # Will be provided by Vault
    GP_HOST: str # Will be provided by Vault
    GP_PORT: str # Will be provided by Vault

    # API settings
    X_SERVICE_NAME: str # Will be provided by Vault
    X_SERVICE_TOKEN: str # Will be provided by Vault
    
    # Feature Store API settings
    FEATURE_STORE_API_URL: str = # Will be provided by Vault 
    
    # LLM Request settings
    LLM_MAX_RETRIES: int = 3
    LLM_RETRY_DELAY: float = 1.0
    LLM_RETRY_MAX_DELAY: float = 10.0
    LLM_REQUEST_TIMEOUT: int = 30
    LLM_MAX_CONCURRENT_REQUESTS: int = 50

    # Error handling
    HANDLE_502_AS_RETRY: bool = True
    RETRY_STATUS_CODES: set[int] = {408, 429, 502, 503, 504}

    MAX_PAGES_SIZE: int = 2

    class Config:
        case_sensitive = True
        env_file = ".env"
        env_file_encoding = 'utf-8'

    @validator('OPENAI_API_KEY')
    def strip_api_key(cls, v: str) -> str:
        """Strip whitespace from API key."""
        return v.strip()

    @validator('RETRY_STATUS_CODES')
    def validate_retry_codes(cls, v):
        """Ensure retry status codes are valid HTTP status codes."""
        if not all(300 <= code <= 599 for code in v):
            raise ValueError("Retry status codes must be between 300 and 599")
        return v

    @validator('LLM_MAX_RETRIES')
    def validate_max_retries(cls, v):
        """Ensure max retries is a positive integer."""
        if v < 0:
            raise ValueError("Max retries must be non-negative")
        return v

settings = Settings() 