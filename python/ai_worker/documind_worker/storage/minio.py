import tempfile
from pathlib import Path

from minio import Minio

from documind_worker.config import settings
from documind_worker.logging import get_logger

logger = get_logger(__name__)

_client: Minio | None = None


def _get_client() -> Minio:
    global _client
    if _client is not None:
        return _client

    _client = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_use_ssl,
    )
    logger.info("minio_client_ready", endpoint=settings.minio_endpoint)
    return _client


def download_to_tempfile(bucket: str, key: str) -> Path:
    """
    Скачивает объект из MinIO во временный файл.
    Возвращает путь к нему. Вызывающий должен удалить файл.
    """
    client = _get_client()

    suffix = Path(key).suffix or ".bin"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.close()
    tmp_path = Path(tmp.name)

    client.fget_object(bucket, key, str(tmp_path))
    logger.info("file_downloaded", bucket=bucket, key=key, size=tmp_path.stat().st_size)
    return tmp_path


def upload_file(local_path: Path, bucket: str, key: str, content_type: str) -> None:
    client = _get_client()
    client.fput_object(bucket, key, str(local_path), content_type=content_type)
    logger.info("file_uploaded", bucket=bucket, key=key)