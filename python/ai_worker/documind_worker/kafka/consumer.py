import asyncio
import json
from typing import Any
from uuid import UUID

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError
from pydantic import ValidationError

from documind_worker.config import settings
from documind_worker.kafka import idempotency
from documind_worker.kafka.producer import publish
from documind_worker.kafka.schemas import DocumentUploadedEvent
from documind_worker.logging import get_logger
from documind_worker.processing import chunking, embeddings, extract
from documind_worker.storage import minio as minio_storage
from documind_worker.storage import pg

logger = get_logger(__name__)


class DocumentConsumer:
    def __init__(self) -> None:
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            settings.kafka_topic_documents,
            bootstrap_servers=settings.kafka_brokers,
            group_id=settings.kafka_group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,   # коммитим вручную после обработки
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            max_poll_records=10,
        )
        await self._consumer.start()
        logger.info(
            "kafka_consumer_started",
            topic=settings.kafka_topic_documents,
            group=settings.kafka_group_id,
        )

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
            logger.info("kafka_consumer_stopped")

    async def run(self) -> None:
        if self._consumer is None:
            raise RuntimeError("consumer is not started")

        try:
            async for msg in self._consumer:
                await self._handle_message(msg)
        except asyncio.CancelledError:
            logger.info("kafka_consumer_cancelled")
            raise

    async def _handle_message(self, msg: Any) -> None:
        event_id = "unknown"
        document_id = "unknown"

        try:
            raw = msg.value
            event = DocumentUploadedEvent.model_validate(raw)
            event_id = str(event.event_id)
            document_id = str(event.payload.document_id)

            logger.info(
                "event_received",
                event_id=event_id,
                document_id=document_id,
                topic=msg.topic,
                partition=msg.partition,
                offset=msg.offset,
            )

            # Идемпотентность
            if await idempotency.is_processed(event_id):
                logger.info("event_already_processed", event_id=event_id)
                await self._commit(msg)
                return

            # Обработка
            await self._process(event)

            # Помечаем как обработанное
            await idempotency.mark_processed(event_id)
            await self._commit(msg)

            logger.info("event_processed", event_id=event_id, document_id=document_id)

        except ValidationError as e:
            # Невалидный payload — permanent error, в DLQ
            logger.error("invalid_event_payload", event_id=event_id, error=str(e))
            await self._send_to_dlq(raw, event_id, document_id, f"validation error: {e}")
            await self._commit(msg)

        except Exception as e:
            logger.exception("event_processing_failed", event_id=event_id, document_id=document_id)
            await self._send_to_dlq(raw, event_id, document_id, str(e))
            await self._commit(msg)

    async def _process(self, event: DocumentUploadedEvent) -> None:
        document_id = str(event.payload.document_id)
        bucket = event.payload.storage.bucket
        key = event.payload.storage.key

        # 1. Создаём/обновляем запись документа
        await pg.create_document(
            document_id=document_id,
            title=event.payload.title,
            content_type=event.payload.content_type,
        )
        await pg.update_document_status(document_id, "processing")

        # 2. Скачиваем файл
        tmp_path = minio_storage.download_to_tempfile(bucket, key)

        try:
            # 3. Извлекаем текст
            pages = extract.extract_text(tmp_path, event.payload.content_type)
            if not pages:
                raise ValueError("no text extracted from file")

            total_chars = sum(len(t) for _, t in pages)
            logger.info(
                "text_extracted",
                document_id=document_id,
                pages=len(pages),
                chars=total_chars,
            )

            # 4. Чанкинг
            chunks = chunking.chunk_pages(pages)
            logger.info("chunked", document_id=document_id, num_chunks=len(chunks))

            # 5. Эмбеддинги
            texts = [c for _, c in chunks]
            vectors = embeddings.embed_passages(texts)

            # 6. Запись в pgvector
            payload = [(page, text, vec) for (page, text), vec in zip(chunks, vectors)]
            inserted = await pg.insert_chunks(document_id, payload)

            await pg.update_document_status(document_id, "ready")
            logger.info("document_processed", document_id=document_id, chunks=inserted)

        except Exception as e:
            await pg.update_document_status(document_id, "failed", str(e))
            raise
        finally:
            tmp_path.unlink(missing_ok=True)

    async def _send_to_dlq(
        self,
        raw: Any,
        event_id: str,
        document_id: str,
        error: str,
    ) -> None:
        from datetime import datetime, timezone

        dlq_event = {
            "event_id": str(UUID(int=0)),  # плейсхолдер
            "event_type": "document.failed",
            "original_event_id": event_id,
            "document_id": document_id,
            "error": error[:1000],
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "retry_count": 0,
        }
        try:
            await publish(
                topic=settings.kafka_topic_dlq,
                key=document_id,
                value=dlq_event,
            )
            logger.warning("event_sent_to_dlq", event_id=event_id, document_id=document_id)
        except Exception:
            logger.exception("dlq_publish_failed", event_id=event_id)

    async def _commit(self, msg: Any) -> None:
        """Коммитим offset после успешной обработки."""
        if self._consumer is None:
            return
        tp = TopicPartition(msg.topic, msg.partition)
        await self._consumer.commit({tp: msg.offset + 1})