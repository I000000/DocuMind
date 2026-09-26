from langchain_text_splitters import RecursiveCharacterTextSplitter

from documind_worker.logging import get_logger

logger = get_logger(__name__)

CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200


def chunk_text(text: str) -> list[str]:
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        length_function=len,
        separators=["\n\n", "\n", ". ", "! ", "? ", "… ", " ", ""],
    )
    return splitter.split_text(text)


def chunk_pages(pages: list[tuple[int | None, str]]) -> list[tuple[int | None, str]]:
    """
    Принимает [(page, text)], возвращает [(page, chunk)].
    Номер страницы сохраняется для цитирования.
    """
    result: list[tuple[int | None, str]] = []
    for page_num, text in pages:
        for chunk in chunk_text(text):
            result.append((page_num, chunk))
    logger.debug("chunked", total_chars=sum(len(t) for _, t in pages), num_chunks=len(result))
    return result