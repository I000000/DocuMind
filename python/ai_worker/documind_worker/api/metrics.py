from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.responses import Response

# Стандартные счётчики обработки документов.
documents_processed_total = Counter(
    "ai_worker_documents_processed_total",
    "Total number of documents successfully processed.",
    ["status"],  # status: success | failed | permanent_error
)

chunks_created_total = Counter(
    "ai_worker_chunks_created_total",
    "Total number of document chunks created.",
)

chunks_embedded_total = Counter(
    "ai_worker_chunks_embedded_total",
    "Total number of chunks with computed embeddings.",
)

# Гистограмма времени обработки одного документа.
processing_duration_seconds = Histogram(
    "ai_worker_processing_duration_seconds",
    "Duration of document processing in seconds.",
    buckets=[0.5, 1, 2, 5, 10, 30, 60, 120],
)


async def metrics_endpoint() -> Response:
    """Возвращает снимок метрик в формате Prometheus."""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )