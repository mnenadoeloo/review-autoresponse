from typing import Dict, Any, Optional, List
import asyncio
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass
from app.core.config import settings

logger = logging.getLogger(__name__)

@dataclass
class QueuedMessage:
    """Message in the retry queue."""
    id: str
    payload: Dict[str, Any]
    retry_count: int = 0
    last_attempt: Optional[datetime] = None
    next_attempt: Optional[datetime] = None

class MessageQueue:
    """Queue for handling failed messages with retry logic."""
    
    def __init__(self):
        self.queue: Dict[str, QueuedMessage] = {}
        self.processing = False
        self._lock = asyncio.Lock()
    
    async def add_message(self, message_id: str, payload: Dict[str, Any]) -> None:
        """Add a message to the retry queue."""
        async with self._lock:
            if message_id not in self.queue:
                self.queue[message_id] = QueuedMessage(
                    id=message_id,
                    payload=payload,
                    next_attempt=datetime.now() + timedelta(
                        seconds=settings.LLM_RETRY_DELAY
                    )
                )
                logger.info(f"Added message {message_id} to retry queue")
    
    async def get_ready_messages(self) -> List[QueuedMessage]:
        """Get messages that are ready for retry."""
        now = datetime.now()
        async with self._lock:
            ready_messages = [
                msg for msg in self.queue.values()
                if msg.next_attempt and msg.next_attempt <= now
                and msg.retry_count < settings.LLM_MAX_RETRIES
            ]
            return ready_messages
    
    async def mark_success(self, message_id: str) -> None:
        """Remove successfully processed message from queue."""
        async with self._lock:
            if message_id in self.queue:
                del self.queue[message_id]
                logger.info(f"Message {message_id} successfully processed and removed from queue")
    
    async def mark_failure(self, message_id: str) -> None:
        """Mark message as failed and update retry timing."""
        async with self._lock:
            if message_id in self.queue:
                msg = self.queue[message_id]
                msg.retry_count += 1
                msg.last_attempt = datetime.now()
                
                if msg.retry_count >= settings.LLM_MAX_RETRIES:
                    logger.error(f"Message {message_id} exceeded max retries, removing from queue")
                    del self.queue[message_id]
                else:
                    # Exponential backoff
                    delay = min(
                        settings.LLM_RETRY_DELAY * (2 ** (msg.retry_count - 1)),
                        settings.LLM_RETRY_MAX_DELAY
                    )
                    msg.next_attempt = datetime.now() + timedelta(seconds=delay)
                    logger.warning(
                        f"Message {message_id} failed, retry {msg.retry_count} "
                        f"scheduled for {msg.next_attempt}"
                    )
    
    def get_queue_stats(self) -> Dict[str, int]:
        """Get current queue statistics."""
        return {
            "total_messages": len(self.queue),
            "retry_pending": sum(1 for msg in self.queue.values() if msg.next_attempt > datetime.now()),
            "max_retries_reached": sum(1 for msg in self.queue.values() if msg.retry_count >= settings.LLM_MAX_RETRIES)
        } 