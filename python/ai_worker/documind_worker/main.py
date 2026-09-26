import asyncio
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from documind_worker.api import health
from documind_worker.config import settings
from documind_worker.logging import get_logger, setup_logging

setup_logging(settings.log_level)
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("worker_starting", env=settings.app_env)
    # TODO: запустить Kafka consumer
    yield
    logger.info("worker_stopping")


app = FastAPI(title="DocuMind AI Worker", lifespan=lifespan)
app.include_router(health.router)


if __name__ == "__main__":
    uvicorn.run(
        "documind_worker.main:app",
        host="0.0.0.0",
        port=settings.server_port,
        reload=True,
    )