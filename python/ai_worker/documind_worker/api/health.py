from fastapi import APIRouter

router = APIRouter()


@router.get("/health/live")
async def live():
    return {"status": "ok"}


@router.get("/health/ready")
async def ready():
    # TODO: проверить Kafka, Postgres, MinIO
    return {"status": "ready"}