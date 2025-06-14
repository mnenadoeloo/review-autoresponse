import asyncpg
import json
import logging
import asyncio
from typing import Dict, Set, Any, Optional
from app.core.config import settings

logger = logging.getLogger(__name__)

class BaseRepository:
    """Base repository class with common functionality."""
    
    def __init__(self) -> None:
        """Initialize base repository."""
        self.pool = None
        self.semaphore = asyncio.Semaphore(10)  # Ограничиваем количество одновременных подключений

class PostgresRepository(BaseRepository):
    """Repository for interacting with PostgreSQL database."""

    def __init__(self, config: Any = None) -> None:
        """Initialize PostgreSQL repository."""
        super().__init__()
        
        if config:
            # Используем объект конфигурации
            self.host = config.host
            self.port = config.port
            self.database = config.database
            self.user = config.user
            self.password = config.password
            self.ssl = config.sslmode != "disable"
        else:
            # Используем настройки из settings
            self.host = settings.DB_HOST
            self.port = settings.DB_PORT
            self.database = settings.DB_NAME
            self.user = settings.DB_USER
            self.password = settings.DB_PASSWORD
            self.ssl = settings.DB_SSL != "disable"

        self.connection_params = {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
            "ssl": self.ssl,
        }

    async def init_pool(self):
        """Initialize the connection pool."""
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                host=self.connection_params["host"],
                port=self.connection_params["port"],
                database=self.connection_params["database"],
                user=self.connection_params["user"],
                password=self.connection_params["password"],
                ssl=False,  # Explicitly disable SSL
                command_timeout=60,
                server_settings={"client_encoding": "UTF8", "timezone": "UTC"},
            )

    async def close_pool(self):
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()
            self.pool = None
    
    async def insert_to_postgres(self, inserted_values: Dict[str, Any]):
        """Inserting values to Postgres DB.

        Args:
            inserted_values: Dictionary with predefined keys.
        """
        async with self.semaphore:
            if not self.pool:
                await self.init_pool()

        try:
            # Открываем прямое соединение с базой данных, а не из пула
            conn = await asyncpg.connect(
                host=self.connection_params["host"],
                port=self.connection_params["port"],
                database=self.connection_params["database"],
                user=self.connection_params["user"],
                password=self.connection_params["password"],
                ssl=False,
                command_timeout=60,
            )
            
            try:
                # Явно устанавливаем режим не только для чтения
                await conn.execute("SET TRANSACTION READ WRITE")
                
                # Выполняем вставку
                await conn.execute(
                    "INSERT INTO table_name(...) VALUES(...)",
                        inserted_values['...'],
                        inserted_values['...'],
                        inserted_values['...'],
                        inserted_values['...'],
                        inserted_values['...'],
                        int(inserted_values['...'])
                    )
                
                logger.info(f"Successfully logged feedback ID {inserted_values['...']} to PostgreSQL")
            except Exception as e:
                logger.error(f"Error during INSERT operation: {str(e)}")
            finally:
                # Закрываем соединение
                await conn.close()

        except Exception as e:
            logger.error(f"Error logging response to Postgres: {str(e)}")
            
    async def insert_to_postgres_alternative(self, inserted_values: Dict[str, Any]):
        """Альтернативный метод вставки данных в Postgres с явными параметрами транзакции.
        
        Args:
            inserted_values: Dictionary with predefined keys.
        """
        try:
            # Открываем прямое соединение с базой данных
            conn = await asyncpg.connect(
                host=self.connection_params["host"],
                port=self.connection_params["port"],
                database=self.connection_params["database"],
                user=self.connection_params["user"],
                password=self.connection_params["password"],
                ssl=False,
                command_timeout=60,
            )
            
            try:
                # Начинаем транзакцию с явными параметрами
                tr = conn.transaction()
                await tr.start()
                
                # Устанавливаем режим READ WRITE для транзакции
                await conn.execute("SET TRANSACTION READ WRITE")
                
                # Выполняем вставку
                await conn.execute(
                    "INSERT INTO table_name(...) VALUES(...)",
                        inserted_values['...'],
                        inserted_values['...'],
                        inserted_values['...'],
                        inserted_values['...'],
                        inserted_values['...'],
                        int(inserted_values['...'])
                    )
                
                # Коммитим транзакцию
                await tr.commit()
                logger.info(f"Successfully logged feedback ID {inserted_values['...']} to PostgreSQL")
            except Exception as e:
                # В случае ошибки, логируем и откатываем транзакцию
                if tr and not tr._done:
                    await tr.rollback()
                logger.error(f"Error in alternative method: {str(e)}")
            finally:
                # Закрываем соединение
                await conn.close()
                
        except Exception as e:
            logger.error(f"Error establishing direct connection to Postgres: {str(e)}")