from asyncio import Semaphore

class BaseRepository:
    """Base class for database repositories.

    Provides common functionality for:
    - Connection pooling
    - Query concurrency limiting
    - Resource cleanup

    Attributes:
        pool: Database connection pool
        semaphore: Async semaphore for limiting concurrent queries
        max_concurrent_queries: Maximum number of concurrent database queries
    """

    def __init__(self, max_concurrent_queries: int = 10) -> None:
        """Initialize base repository.

        Args:
            max_concurrent_queries: Maximum number of concurrent database queries.
                Defaults to 10 to prevent overloading the database.
        """
        self.pool = None
        self.semaphore = Semaphore(max_concurrent_queries)
        self.max_concurrent_queries = max_concurrent_queries

    async def init_pool(self):
        """Initialize the connection pool.

        This method should be implemented by child classes.
        """
        raise NotImplementedError

    async def close_pool(self):
        """Close the connection pool.

        This method should be implemented by child classes.
        """
        raise NotImplementedError

    async def __aenter__(self) -> "BaseRepository":
        """Enter async context."""
        await self.init_pool()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context."""
        await self.close_pool()