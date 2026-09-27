from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from documind_worker.api import health, test
from documind_worker.config import settings
from documind_worker.kafka import consumer, idempotency, producer
from documind_worker.logging import get_logger, setup_logging
from documind_worker.storage import pg

setup_logging(settings.log_level)
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("worker_starting", env=settings.app_env)

    await pg.init_pool()
    await idempotency.init_client()
    await producer.start_producer()

    doc_consumer = consumer.DocumentConsumer()
    await doc_consumer.start()

    import asyncio
    consume_task = asyncio.create_task(doc_consumer.run())

    yield

    logger.info("worker_stopping")
    consume_task.cancel()
    try:
        await consume_task
    except asyncio.CancelledError:
        pass

    await doc_consumer.stop()
    await producer.stop_producer()
    await idempotency.close_client()
    await pg.close_pool()


app = FastAPI(title="DocuMind AI Worker", lifespan=lifespan)
app.include_router(health.router)
app.include_router(test.router, prefix="/test", tags=["test"])


if __name__ == "__main__":
    uvicorn.run(
        "documind_worker.main:app",
        host="0.0.0.0",
        port=settings.server_port,
        reload=True,
    )