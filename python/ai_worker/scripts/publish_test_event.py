"""
Тестовый скрипт: публикует событие document.uploaded в Kafka.
Использование:
    python scripts/publish_test_event.py <path-to-file>
"""
import asyncio
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from minio import Minio

from documind_worker.config import settings
from documind_worker.kafka import producer as kafka_producer
from documind_worker.logging import setup_logging

setup_logging("INFO")


async def main(file_path: Path) -> None:
    # 1. Загружаем файл в MinIO
    client = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_use_ssl,
    )

    document_id = str(uuid.uuid4())
    suffix = file_path.suffix
    object_key = f"{document_id}{suffix}"

    client.fput_object(
        bucket_name=settings.minio_bucket,
        object_name=object_key,
        file_path=str(file_path),
        content_type="application/pdf" if suffix == ".pdf" else "application/octet-stream",
    )
    print(f"Uploaded to MinIO: {settings.minio_bucket}/{object_key}")

    # 2. Публикуем событие
    await kafka_producer.start_producer()

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "document.uploaded",
        "version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "trace_id": None,
        "request_id": "test-script",
        "payload": {
            "document_id": document_id,
            "title": file_path.name,
            "content_type": "application/pdf" if suffix == ".pdf" else "application/octet-stream",
            "size_bytes": file_path.stat().st_size,
            "storage": {
                "provider": "minio",
                "bucket": settings.minio_bucket,
                "key": object_key,
            },
            "uploaded_by": {
                "user_id": None,
                "email": "test@documind.local",
            },
        },
    }

    await kafka_producer.publish(
        topic=settings.kafka_topic_documents,
        key=document_id,
        value=event,
    )
    print(f"Published event for document_id={document_id}")

    await kafka_producer.stop_producer()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/publish_test_event.py <file>")
        sys.exit(1)

    file_path = Path(sys.argv[1])
    if not file_path.exists():
        print(f"File not found: {file_path}")
        sys.exit(1)

    asyncio.run(main(file_path))