import threading

import torch
from sentence_transformers import CrossEncoder

from documind_worker.config import settings
from documind_worker.logging import get_logger

logger = get_logger(__name__)

RERANKER_MODEL = "DiTy/cross-encoder-russian-msmarco"

_model: CrossEncoder | None = None
_lock = threading.Lock()


def _get_model() -> CrossEncoder:
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

        logger.info("loading_reranker_model", model=RERANKER_MODEL, device=device)
        _model = CrossEncoder(
            RERANKER_MODEL,
            device=device,
            max_length=512,
        )
        logger.info("reranker_model_loaded")
        return _model


def rerank(
    query: str,
    passages: list[str],
    top_k: int = 5,
) -> list[tuple[int, float]]:
    """
    Пересчитывает релевантность пар (query, passage).
    Возвращает список (original_index, score), отсортированный по убыванию score.
    """
    if not passages:
        return []

    model = _get_model()
    pairs = [(query, p) for p in passages]

    with torch.inference_mode():
        scores = model.predict(
            pairs,
            batch_size=8,
            show_progress_bar=False,
            convert_to_numpy=True,
        )

    indexed = list(enumerate(scores.tolist()))
    indexed.sort(key=lambda x: x[1], reverse=True)
    return indexed[:top_k]