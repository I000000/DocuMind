from pathlib import Path

import pytesseract
from docx import Document
from pdf2image import convert_from_path
from pypdf import PdfReader

from documind_worker.config import settings
from documind_worker.logging import get_logger

pytesseract.pytesseract.tesseract_cmd = settings.tesseract_cmd

logger = get_logger(__name__)


class UnsupportedFormatError(Exception):
    pass


def extract_text(file_path: Path, content_type: str) -> list[tuple[int | None, str]]:
    """
    Извлекает текст из файла.
    Возвращает список (номер_страницы | None, текст).
    """
    suffix = file_path.suffix.lower()

    if suffix == ".pdf" or content_type == "application/pdf":
        return _extract_pdf(file_path)
    if suffix == ".docx":
        return _extract_docx(file_path)
    if suffix in (".txt", ".md") or content_type in ("text/plain", "text/markdown"):
        return _extract_text_file(file_path)

    raise UnsupportedFormatError(f"unsupported format: {suffix} ({content_type})")


def _extract_pdf(path: Path) -> list[tuple[int | None, str]]:
    reader = PdfReader(str(path))
    pages: list[tuple[int | None, str]] = []

    for page_num, page in enumerate(reader.pages, start=1):
        text = (page.extract_text() or "").strip()

        if len(text) < 50:
            logger.info("page_needs_ocr", page=page_num)
            text = _ocr_page(path, page_num)

        if text:
            pages.append((page_num, text))

    return pages


def _ocr_page(pdf_path: Path, page_num: int) -> str:
    try:
        images = convert_from_path(
            str(pdf_path),
            first_page=page_num,
            last_page=page_num,
            dpi=200,
        )
    except Exception as e:
        logger.error("pdf_to_image_failed", page=page_num, error=str(e))
        return ""

    if not images:
        return ""

    return pytesseract.image_to_string(images[0], lang=settings.ocr_languages).strip()


def _extract_docx(path: Path) -> list[tuple[int | None, str]]:
    doc = Document(str(path))
    parts = [p.text for p in doc.paragraphs if p.text.strip()]
    return [(None, "\n".join(parts))]


def _extract_text_file(path: Path) -> list[tuple[int | None, str]]:
    return [(None, path.read_text(encoding="utf-8", errors="replace"))]