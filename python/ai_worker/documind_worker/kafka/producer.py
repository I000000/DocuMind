import json
from typing import Any

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from documind_worker.config import settings
from documind_worker.logging import get_logger

logger = get_logger(__name__)

_producer: AIOKafkaProducer | None = None


async def start_producer() -> None:
    global _producer
    if _producer is not None:
        return

    _producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_brokers,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",              # ждём подтверждения от всех реплик
        enable_idempotence=True, # exactly-once для producer
        compression_type="gzip",
    )
    await _producer.start()
    logger.info("kafka_producer_started", brokers=settings.kafka_brokers)


async def stop_producer() -> None:
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None
        logger.info("kafka_producer_stopped")


async def publish(
    topic: str,
    key: str | None,
    value: dict[str, Any],
    headers: dict[str, str] | None = None,
) -> None:
    """Публикует событие в топик. key обычно = document_id или event_id."""
    if _producer is None:
        raise RuntimeError("kafka producer is not started")

    kafka_headers = [(k, v.encode("utf-8")) for k, v in (headers or {}).items()]

    try:
        await _producer.send_and_wait(topic, value=value, key=key, headers=kafka_headers)
        logger.info("event_published", topic=topic, key=key, event_type=value.get("event_type"))
    except KafkaError as e:
        logger.error("event_publish_failed", topic=topic, key=key, error=str(e))
        raise