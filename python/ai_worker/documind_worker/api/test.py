import tempfile
import uuid
from pathlib import Path

from fastapi import APIRouter, File, HTTPException, UploadFile

from documind_worker.logging import get_logger
from documind_worker.processing import chunking, embeddings, extract
from documind_worker.storage import pg

logger = get_logger(__name__)
router = APIRouter()


@router.post("/process")
async def process_file(file: UploadFile = File(...)):
    """Тестовый эндпоинт. Будет заменён на Kafka-обработку."""
    doc_id = str(uuid.uuid4())
    content_type = file.content_type or "application/octet-stream"
    suffix = Path(file.filename or "file").suffix

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await file.read())
        tmp_path = Path(tmp.name)

    logger.info(
        "processing_started",
        document_id=doc_id,
        filename=file.filename,
        content_type=content_type,
        size=tmp_path.stat().st_size,
    )

    try:
        await pg.create_document(doc_id, file.filename or "Untitled", content_type)
        await pg.update_document_status(doc_id, "processing")

        pages = extract.extract_text(tmp_path, content_type)
        if not pages:
            raise HTTPException(400, "no text extracted from file")

        total_chars = sum(len(t) for _, t in pages)
        logger.info("text_extracted", document_id=doc_id, pages=len(pages), chars=total_chars)

        chunks = chunking.chunk_pages(pages)
        logger.info("chunked", document_id=doc_id, num_chunks=len(chunks))

        texts = [c for _, c in chunks]
        vectors = embeddings.embed_passages(texts)
        logger.info("embedded", document_id=doc_id, count=len(vectors))

        payload = [(page, text, vec) for (page, text), vec in zip(chunks, vectors)]
        inserted = await pg.insert_chunks(doc_id, payload)
        await pg.update_document_status(doc_id, "ready")

        return {
            "document_id": doc_id,
            "status": "ready",
            "pages": len(pages),
            "chunks": inserted,
            "chars": total_chars,
        }

    except HTTPException:
        await pg.update_document_status(doc_id, "failed", "validation error")
        raise
    except Exception as e:
        logger.exception("processing_failed", document_id=doc_id)
        await pg.update_document_status(doc_id, "failed", str(e))
        raise HTTPException(500, f"processing failed: {e}")
    finally:
        tmp_path.unlink(missing_ok=True)