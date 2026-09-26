import asyncpg
import numpy as np
from pgvector.asyncpg import register_vector

from documind_worker.config import settings
from documind_worker.logging import get_logger

logger = get_logger(__name__)

_pool: asyncpg.Pool | None = None


async def init_pool() -> None:
    global _pool
    if _pool is not None:
        return

    logger.info("postgres_pool_creating", host=settings.db_host, db=settings.db_name)
    _pool = await asyncpg.create_pool(
        dsn=settings.db_dsn,
        min_size=2,
        max_size=10,
        command_timeout=60,
        init=_init_connection,
    )
    logger.info("postgres_pool_ready")


async def _init_connection(conn: asyncpg.Connection) -> None:
    await register_vector(conn)


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("postgres_pool_closed")


def pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("postgres pool is not initialized")
    return _pool


async def create_document(
    document_id: str,
    title: str,
    content_type: str,
    source: str | None = None,
) -> None:
    async with pool().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO documents (id, title, content_type, source, status)
            VALUES ($1, $2, $3, $4, 'pending')
            ON CONFLICT (id) DO NOTHING
            """,
            document_id, title, content_type, source,
        )


async def update_document_status(
    document_id: str,
    status: str,
    error_message: str | None = None,
) -> None:
    async with pool().acquire() as conn:
        await conn.execute(
            """
            UPDATE documents
            SET status = $1, error_message = $2, updated_at = NOW()
            WHERE id = $3
            """,
            status, error_message, document_id,
        )


async def insert_chunks(
    document_id: str,
    chunks: list[tuple[int | None, str, list[float]]],
) -> int:
    if not chunks:
        return 0

    async with pool().acquire() as conn:
        async with conn.transaction():
            for idx, (page, content, embedding) in enumerate(chunks):
                await conn.execute(
                    """
                    INSERT INTO document_chunks
                        (document_id, chunk_index, content, page, embedding)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    document_id, idx, content, page,
                    np.array(embedding, dtype=np.float32),
                )

    logger.info("chunks_inserted", document_id=document_id, count=len(chunks))
    return len(chunks)