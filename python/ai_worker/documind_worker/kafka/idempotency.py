import redis.asyncio as redis

from documind_worker.config import settings
from documind_worker.logging import get_logger

logger = get_logger(__name__)

_client: redis.Redis | None = None


async def init_client() -> None:
    global _client
    if _client is not None:
        return

    _client = redis.from_url(
        f"redis://{settings.redis_addr}/{settings.redis_db}",
        password=settings.redis_password or None,
        decode_responses=True,
    )
    await _client.ping()
    logger.info("redis_client_ready", addr=settings.redis_addr)


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
        logger.info("redis_client_closed")


def _key(event_id: str) -> str:
    return f"processed_event:{event_id}"


async def is_processed(event_id: str) -> bool:
    """Проверяет, обработано ли событие раньше."""
    if _client is None:
        raise RuntimeError("redis client is not initialized")
    return await _client.exists(_key(event_id)) == 1


async def mark_processed(event_id: str) -> None:
    """Помечает событие как обработанное с TTL."""
    if _client is None:
        raise RuntimeError("redis client is not initialized")
    ttl_seconds = settings.idempotency_ttl_hours * 3600
    await _client.setex(_key(event_id), ttl_seconds, "1")