import threading

import torch
from sentence_transformers import SentenceTransformer

from documind_worker.config import settings
from documind_worker.logging import get_logger

logger = get_logger(__name__)

_model: SentenceTransformer | None = None
_lock = threading.Lock()


def _get_model() -> SentenceTransformer:
    global _model
    if _model is not None:
        return _model

    with _lock:
        if _model is not None:
            return _model

        device = settings.embedding_device
        if device == "cuda" and not torch.cuda.is_available():
            logger.warning("cuda_unavailable_falling_back_to_cpu")
            device = "cpu"

        logger.info("loading_embedding_model", model=settings.embedding_model, device=device)
        _model = SentenceTransformer(settings.embedding_model, device=device)
        _model.eval()
        logger.info("embedding_model_loaded")
        return _model


def embed_passages(texts: list[str]) -> list[list[float]]:
    """Считает эмбеддинги для чанков документа. E5 требует префикс 'passage: '."""
    if not texts:
        return []

    model = _get_model()
    prefixed = [f"passage: {t}" for t in texts]

    with torch.inference_mode():
        embeddings = model.encode(
            prefixed,
            batch_size=settings.embedding_batch_size,
            normalize_embeddings=True,
            show_progress_bar=False,
            convert_to_numpy=True,
        )

    return embeddings.tolist()


def embed_query(text: str) -> list[float]:
    """Эмбеддинг поискового запроса. E5 требует префикс 'query: '."""
    model = _get_model()
    with torch.inference_mode():
        embedding = model.encode(
            [f"query: {text}"],
            normalize_embeddings=True,
            show_progress_bar=False,
            convert_to_numpy=True,
        )
    return embedding[0].tolist()


def get_embedding_dim() -> int:
    return _get_model().get_sentence_embedding_dimension()