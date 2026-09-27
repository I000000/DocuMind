# Event: `documind.documents.v1` — DocumentUploaded

**Version:** v1
**Topic:** `documind.documents.v1`
**Producer:** Go Document Service
**Consumer:** Python AI Worker (group: `ai-worker-group`)

## Purpose

Уведомляет о загрузке нового документа. AI Worker скачивает файл из MinIO,
обрабатывает (OCR → chunk → embed) и пишет результат в pgvector.

## Payload (JSON)

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_type": "document.uploaded",
  "version": 1,
  "occurred_at": "2026-09-26T18:00:00Z",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "request_id": "req-abc-123",
  "payload": {
    "document_id": "660f8400-e29b-41d4-a716-446655440001",
    "title": "Отчёт за Q3",
    "content_type": "application/pdf",
    "size_bytes": 245811,
    "storage": {
      "provider": "minio",
      "bucket": "documents",
      "key": "660f8400-e29b-41d4-a716-446655440001.pdf"
    },
    "uploaded_by": {
      "user_id": "0402bf48-81da-4926-8465-5b6c26f773b1",
      "email": "editor@documind.local"
    }
  }
}