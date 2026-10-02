import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, TopicPartition
from pydantic import ValidationError

from documind_worker.api import metrics
from documind_worker.config import settings
from documind_worker.kafka import idempotency
from documind_worker.kafka.producer import publish
from documind_worker.kafka.schemas import DocumentUploadedEvent
from documind_worker.logging import get_logger
from documind_worker.processing import chunking, embeddings, extract
from documind_worker.storage import minio as minio_storage
from documind_worker.storage import pg

logger = get_logger(__name__)


class PermanentError(Exception):
    """
    Ошибка, при которой retry бессмысленен (невалидный формат, пустой файл).
    Такие ошибки сразу уходят в DLQ без повторов.
    """


class DocumentConsumer:
    """
    Kafka-consumer для топика документов.

    Гарантии:
    - at-least-once: offset коммитится только после успешной обработки.
    - Идемпотентность: повторные события с тем же event_id пропускаются
      (Redis-ключ ставится после успеха, не до).
    - Retry: transient-ошибки ретраятся с экспоненциальным backoff.
    - DLQ: permanent-ошибки и исчерпанные retry уходят в отдельный топик.
    """

    def __init__(self) -> None:
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            settings.kafka_topic_documents,
            bootstrap_servers=settings.kafka_brokers,
            group_id=settings.kafka_group_id,
            auto_offset_reset="earliest",
            # Manual commit: коммитим offset только после успешной обработки,
            # чтобы при падении воркера сообщение не потерялось (at-least-once).
            enable_auto_commit=False,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            # По одному сообщению за poll: обработка тяжёлая (OCR + GPU),
            # батч = 10 займёт несколько минут.
            max_poll_records=1,
            # 10 минут на обработку одного сообщения. Хватает даже на
            # первый запуск, когда модель E5 грузится с диска в GPU.
            max_poll_interval_ms=600_000,
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
        """Основной цикл. Отменяется через CancelledError при shutdown."""
        if self._consumer is None:
            raise RuntimeError("consumer is not started")

        try:
            async for msg in self._consumer:
                await self._handle_message(msg)
        except asyncio.CancelledError:
            logger.info("kafka_consumer_cancelled")
            raise

    async def _handle_message(self, msg: Any) -> None:
        """Диспетчер одного сообщения: валидация → идемпотентность → обработка → DLQ."""
        event_id = "unknown"
        document_id = "unknown"

        try:
            # 1. Валидация. Невалидный payload — permanent error, сразу в DLQ.
            #    Pydantic здесь — это контракт с producer'ом (Go Document Service).
            try:
                event = DocumentUploadedEvent.model_validate(msg.value)
            except ValidationError as e:
                logger.error("invalid_event_payload", error=str(e), raw_keys=list(msg.value.keys()))
                await self._send_to_dlq(
                    raw=msg.value,
                    event_id=event_id,
                    document_id=document_id,
                    error=f"validation error: {e}",
                    retry_count=0,
                )
                return

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

            # 2. Идемпотентность. Kafka гарантирует at-least-once.
            if await idempotency.is_processed(event_id):
                logger.info("event_already_processed", event_id=event_id)
                return

            # 3. Обработка с retry. Ошибки классифицируются на transient/permanent.
            await self._process_with_retry(event)

            # 4. Помечаем как обработанное ДО commit — если commit упадёт,
            #    при переотправке идемпотентность пропустит обработку.
            await idempotency.mark_processed(event_id)
            logger.info("event_processed", event_id=event_id, document_id=document_id)

        except PermanentError as e:
            # Permanent error — retry бессмысленен, сразу в DLQ с retry_count=0.
            logger.error("permanent_error", event_id=event_id, document_id=document_id, error=str(e))
            await self._send_to_dlq(msg.value, event_id, document_id, str(e), retry_count=0)

        except Exception as e:
            # Сюда попадаем после исчерпания retry. Документ остаётся в статусе 'processing'
            logger.exception("retries_exhausted", event_id=event_id, document_id=document_id)
            metrics.documents_processed_total.labels(status="failed").inc()
            await self._send_to_dlq(
                msg.value, event_id, document_id, str(e),
                retry_count=settings.max_retries,
            )

        # Commit — ВСЕГДА в отдельной попытке, независимо от исхода обработки.
        # Если commit упадёт — не отправляем в DLQ, Kafka переотправит сообщение,
        # а идемпотентность (Redis) спасёт от повторной обработки.
        finally:
            await self._safe_commit(msg)

    async def _process_with_retry(self, event: DocumentUploadedEvent) -> None:
        """
        Retry с экспоненциальным backoff.

        PermanentError пробрасывается сразу — без retry.
        Прочие исключения ретраятся до settings.max_retries, потом пробрасываются
        наверх, где уходят в DLQ.
        """
        event_id = str(event.event_id)
        document_id = str(event.payload.document_id)
        last_error: Exception | None = None

        for attempt in range(1, settings.max_retries + 1):
            try:
                await self._process(event)
                return
            except PermanentError:
                raise  # не retry
            except Exception as e:
                last_error = e

                if attempt >= settings.max_retries:
                    logger.error(
                        "max_retries_reached",
                        event_id=event_id,
                        document_id=document_id,
                        attempts=attempt,
                        error=str(e),
                    )
                    break

                # Экспоненциальный backoff: 1s → 2s → 4s → 8s → ..., потолок 30s.
                delay = min(
                    settings.retry_base_delay * (2 ** (attempt - 1)),
                    settings.retry_max_delay,
                )
                logger.warning(
                    "processing_retry_scheduled",
                    event_id=event_id,
                    document_id=document_id,
                    attempt=attempt,
                    max_retries=settings.max_retries,
                    delay_seconds=delay,
                    error=str(e),
                )
                await asyncio.sleep(delay)

        if last_error is not None:
            raise last_error

    async def _process(self, event: DocumentUploadedEvent) -> None:
        """
        Полный пайплайн: скачать из MinIO → извлечь → чанкинг → эмбеддинги → pgvector.

        Классифицирует ошибки:
        - PermanentError → документ помечается 'failed', в DLQ.
        - Прочие → документ остаётся 'processing', будет retry.
        """
        document_id = str(event.payload.document_id)
        bucket = event.payload.storage.bucket
        key = event.payload.storage.key

        start_time = time.monotonic()


        await pg.create_document(
            document_id=document_id,
            title=event.payload.title,
            content_type=event.payload.content_type,
        )
        await pg.update_document_status(document_id, "processing")

        # Скачивание — transient error.
        tmp_path = minio_storage.download_to_tempfile(bucket, key)

        try:
            # Извлечение текста. Неподдерживаемый формат — permanent.
            try:
                pages = extract.extract_text(tmp_path, event.payload.content_type)
            except extract.UnsupportedFormatError as e:
                raise PermanentError(f"unsupported format: {e}") from e

            if not pages:
                raise PermanentError("no text extracted from file")

            total_chars = sum(len(t) for _, t in pages)
            logger.info("text_extracted", document_id=document_id, pages=len(pages), chars=total_chars)

            chunks = chunking.chunk_pages(pages)
            logger.info("chunked", document_id=document_id, num_chunks=len(chunks))

            texts = [c for _, c in chunks]
            vectors = embeddings.embed_passages(texts)

            payload = [(page, text, vec) for (page, text), vec in zip(chunks, vectors)]
            inserted = await pg.insert_chunks(document_id, payload)

            await pg.update_document_status(document_id, "ready")
            logger.info("document_processed", document_id=document_id, chunks=inserted)

            metrics.documents_processed_total.labels(status="success").inc()
            metrics.chunks_created_total.inc(len(chunks))
            metrics.chunks_embedded_total.inc(len(vectors))
            metrics.processing_duration_seconds.observe(time.monotonic() - start_time)

        except PermanentError:
            await pg.update_document_status(document_id, "failed", "permanent error")

            metrics.documents_processed_total.labels(status="permanent_error").inc()
            metrics.processing_duration_seconds.observe(time.monotonic() - start_time)

            raise
        except Exception as e:
            # Transient error — НЕ помечаем 'failed', чтобы при retry можно было
            # перезаписать чанки. Статус останется 'processing' до исхода retry.
            logger.warning("processing_error_will_retry", document_id=document_id, error=str(e))
            # Метрику здесь НЕ увеличиваем — retry может закончиться успехом.
            # Метрика будет в _handle_message, когда исчерпаются все попытки.
            raise
        finally:
            tmp_path.unlink(missing_ok=True)

    async def _send_to_dlq(
        self,
        raw: Any,
        event_id: str,
        document_id: str,
        error: str,
        retry_count: int,
    ) -> None:
        """
        Кладём событие в DLQ-топик.

        Включаем original_event целиком, чтобы инженер мог вручную переиграть
        сообщение через kafka-console-producer в основной топик.
        """
        dlq_event = {
            "event_id": str(uuid4()),
            "event_type": "document.failed",
            "original_event_id": event_id,
            "document_id": document_id,
            "error": error[:2000],  # обрезаем длинные трейсбеки
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "retry_count": retry_count,
            "original_event": raw if isinstance(raw, dict) else None,
        }
        try:
            await publish(
                topic=settings.kafka_topic_dlq,
                key=document_id,
                value=dlq_event,
            )
            logger.warning(
                "event_sent_to_dlq",
                event_id=event_id,
                document_id=document_id,
                retry_count=retry_count,
            )
        except Exception:
            # Если даже DLQ не работает — логируем и идём дальше.
            # Худший сценарий: потеряем событие, но не заблокируем consumer.
            logger.exception("dlq_publish_failed", event_id=event_id)

    async def _safe_commit(self, msg: Any) -> None:
        """
        Пытается закоммитить offset, устойчиво к rebalance.

        Если group generation устарел (worker выпал из группы во время
        долгой обработки) — commit упадёт. Это не ошибка обработки, а
        нормальное поведение Kafka при долгих операциях. Логируем и идём
        дальше: consumer переприсоединится, Kafka доставит сообщение снова,
        идемпотентность через Redis пропустит повторную обработку.
        """
        if self._consumer is None:
            return
        tp = TopicPartition(msg.topic, msg.partition)
        try:
            await self._consumer.commit({tp: msg.offset + 1})
        except Exception as e:
            logger.warning(
                "offset_commit_failed",
                topic=msg.topic,
                partition=msg.partition,
                offset=msg.offset,
                error=str(e),
                hint="consumer will rejoin; message will be redelivered and skipped by idempotency",
            )