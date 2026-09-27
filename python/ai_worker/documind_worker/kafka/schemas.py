from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field


class StorageInfo(BaseModel):
    provider: Literal["minio"] = "minio"
    bucket: str
    key: str


class UploadedBy(BaseModel):
    user_id: str | None = None
    email: str | None = None


class DocumentPayload(BaseModel):
    document_id: UUID
    title: str
    content_type: str
    size_bytes: int
    storage: StorageInfo
    uploaded_by: UploadedBy | None = None


class DocumentUploadedEvent(BaseModel):
    """Событие 'document.uploaded' из топика documind.documents.v1."""

    event_id: UUID
    event_type: Literal["document.uploaded"]
    version: int = 1
    occurred_at: datetime
    trace_id: str | None = None
    request_id: str | None = None
    payload: DocumentPayload

    model_config = {"extra": "ignore"}


class DocumentFailedEvent(BaseModel):
    """Событие 'document.failed' — публикуем в DLQ."""

    event_id: UUID
    event_type: Literal["document.failed"] = "document.failed"
    original_event_id: UUID
    document_id: UUID
    error: str
    occurred_at: datetime
    retry_count: int = 0