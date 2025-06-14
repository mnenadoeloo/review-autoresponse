import psycopg2
from psycopg2.extras import DictCursor
from app.models.llm import NmIdData
import logging
import requests
import json
from typing import Dict, Optional, ClassVar
from app.core.config import settings
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
from app.core.performance_settings import performance_settings
from aiohttp import ClientSession, ClientTimeout
from tenacity import retry, stop_after_attempt, wait_exponential
import asyncio
from cachetools import TTLCache

logger = logging.getLogger(__name__)

parentname2category = {
    "Обувь": "ОБУВЬ",
    "Ноутбуки и компьютеры": "ЭЛЕКТРОНИКА",
    "Спортивная обувь": "ОБУВЬ",
    "Спортивная одежда": "ОДЕЖДА",
    "Смартфоны и аксессуары": "ЭЛЕКТРОНИКА",
    "Спецодежда и СИЗы": "ОДЕЖДА", 
    "Телевизоры и аудиотехника": "ЭЛЕКТРОНИКА",
    "Одежда": "ОДЕЖДА",
    "Текстиль": "ТЕКСТИЛЬ",
    "Текстиль для дома": "ТЕКСТИЛЬ",
    "Шторы и аксессуары": "ТЕКСТИЛЬ",
    "Декор интерьера": "ТЕКСТИЛЬ",
    "Хозяйственные товары": "ТЕКСТИЛЬ",
    "Рукоделие": "ТЕКСТИЛЬ",
    "Хранение вещей": "ТЕКСТИЛЬ"
}

class DatabaseManager:
    """A manager class for handling database operations and API fallbacks.

    This class manages database connections through a connection pool and provides
    fallback mechanisms to fetch data from an API when the database is unavailable.

    Attributes:
        _http_session (ClassVar[Optional[ClientSession]]): Shared HTTP session for API calls.
        _cache (ClassVar[TTLCache]): TTL cache for storing responses.
        db_params (dict): Database connection parameters.
        cards_api_url (str): URL for the cards API endpoint.
        headers (dict): HTTP headers for API requests.
        timeout (ClientTimeout): Timeout configuration for HTTP requests.
        pool (ThreadedConnectionPool): Connection pool for database connections.
        db_available (bool): Flag indicating if database is available.
    """
    
    # Class-level session for reuse
    _http_session: ClassVar[Optional[ClientSession]] = None
    _cache: ClassVar[TTLCache] = TTLCache(maxsize=1000, ttl=3600)  # 1-hour TTL cache
    
    def __init__(self):
        """Initialize the DatabaseManager with connection settings and API configuration."""
        self.db_params = {
            "database": settings.DB_NAME,
            "user": settings.DB_USER,
            "password": settings.DB_PASSWORD,
            "host": settings.DB_HOST,
            "port": settings.DB_PORT,
            "sslmode": settings.DB_SSL
        }
        self.cards_api_url = settings.FEATURE_STORE_API_URL
        self.headers = {
            "Content-Type": "application/json"
        }
        
        # Configure HTTP timeouts
        self.timeout = ClientTimeout(total=10)
        
        # Initialize connection pool with retry settings
        self.pool = None
        self.db_available = False
        try:
            self.pool = ThreadedConnectionPool(
                minconn=5,
                maxconn=performance_settings.DB_POOL_SIZE,
                **self.db_params
            )
            self.db_available = True
        except Exception as e:
            logger.error(f"Failed to initialize database connection pool: {str(e)}")
            logger.info("Will use API fallback for all requests")
    
    @contextmanager
    def get_connection(self):
        """Get a database connection from the pool.

        This context manager handles the safe acquisition and release of database
        connections from the connection pool.

        Yields:
            psycopg2.connection: A database connection from the pool.

        Raises:
            Exception: If the database is not available or the pool is not initialized.
        """
        if not self.db_available or self.pool is None:
            raise Exception("Database is not available")
            
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)

    @classmethod
    async def get_http_session(cls) -> ClientSession:
        """Get or create a shared HTTP session.

        Returns:
            ClientSession: A shared aiohttp ClientSession instance.
        """
        if cls._http_session is None or cls._http_session.closed:
            cls._http_session = ClientSession(timeout=ClientTimeout(total=10))
        return cls._http_session

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def _get_card_from_api(self, nm_id: int) -> Optional[Dict]:
        """Fetch card data from the API with retries and connection pooling.

        Args:
            nm_id (int): The unique identifier of the card to fetch.

        Returns:
            Optional[Dict]: The card data if found, None otherwise.

        Raises:
            Exception: If the API request fails after retries.
        """
        try:
            session = await self.get_http_session()
            data = {"ids": [nm_id]}
            
            async with session.get(
                self.cards_api_url,
                headers=self.headers,
                json=data,
                raise_for_status=True
            ) as response:
                result = await response.json()
                cards = result.get("Cards", [])
                return cards[0] if cards else None
                
        except Exception as e:
            logger.error(f"Failed to fetch data from API for nm_id {nm_id}: {str(e)}")
            raise

    def _extract_card_data(self, card: Dict) -> NmIdData:
        """Extract relevant data from a card dictionary.

        Args:
            card (Dict): Raw card data from the API.

        Returns:
            NmIdData: Structured card data with title, description, and category.
        """
        if not card or 'RawData' not in card:
            return NmIdData(title="", description="")
            
        fields = card['RawData'].get('Fields', [])
        title = ""
        description = ""
        category = "DEFAULT"  # default category
        
        for field in fields:
            if field['Name'] == 'Название':
                title = field['Value']
            elif field['Name'] == 'Описание':
                description = field['Value']
            elif field['Name'] == 'Корневая категория':
                category = parentname2category.get(field['Value'], "DEFAULT")
                
        return NmIdData(
            title=title,
            description=description,
            category=category
        )
    
    async def get_nm_id_data(self, nm_id: int) -> NmIdData:
        """Fetch product data by nm_id from database or API with caching.

        This method first checks the cache for the result. If not found, it tries
        the database, and if that fails, uses the API as a fallback. The obtained
        result is then cached.
        """
        # Check if the result is already cached
        if nm_id in self.__class__._cache:
            return self.__class__._cache[nm_id]
        
        data = None
        
        if self.db_available:
            try:
                async with asyncio.timeout(5.0):  # Timeout for the entire operation
                    with self.get_connection() as conn:
                        with conn.cursor(cursor_factory=DictCursor) as cursor:
                            cursor.execute("""
                                SELECT ...
                                FROM ...
                                WHERE nmid = %s
                                LIMIT 1;
                            """, (nm_id,))
                            
                            result = cursor.fetchone()
                            if result:
                                data = NmIdData(
                                    title=result['title'] or "",
                                    description=result['description'] or "",
                                    category=parentname2category.get(result['parentname'], "НЕИЗВЕСТНО")
                                )
            except Exception as e:
                logger.error(f"Database error for nm_id {nm_id}: {str(e)}")
        
        if data is None:
            # Fallback to API if DB is not available or query failed
            try:
                card = await self._get_card_from_api(nm_id)
                if card:
                    data = self._extract_card_data(card)
            except Exception as e:
                logger.error(f"API fallback failed for nm_id {nm_id}: {str(e)}")
        
        if data is None:
            data = NmIdData(title="", description="", category="НЕИЗВЕСТНО")
        
        # Cache the result
        self.__class__._cache[nm_id] = data
        return data

    async def _fetch_product_data_batch(
        self,
        nm_ids: set[int]
    ) -> Dict[int, NmIdData]:
        """Fetch product data for multiple nm_ids efficiently."""
        # Create tasks for each nm_id
        tasks = {
            nm_id: asyncio.create_task(self.get_nm_id_data(nm_id))
            for nm_id in nm_ids
        }
        
        # Wait for all tasks to complete
        await asyncio.gather(*tasks.values())
        
        # Collect results
        return {
            nm_id: task.result()
            for nm_id, task in tasks.items()
        }