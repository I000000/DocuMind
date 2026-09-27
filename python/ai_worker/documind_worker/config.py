from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = "development"
    log_level: str = "INFO"
    server_port: int = 8002

    # Testing / debugging
    simulate_transient_error: bool = False

    # Kafka
    kafka_brokers: str = "localhost:9092"
    kafka_topic_documents: str = "documind.documents.v1"
    kafka_topic_dlq: str = "documind.documents.v1.dlq"
    kafka_group_id: str = "ai-worker-group"
    kafka_consumer_workers: int = 3

    # Retry
    max_retries: int = 5
    retry_base_delay: float = 1.0
    retry_max_delay: float = 30.0

    # PostgreSQL
    db_host: str = "localhost"
    db_port: int = 5432
    db_user: str = "documind"
    db_password: str = "documind"
    db_name: str = "documind"

    # MinIO
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "documind"
    minio_secret_key: str = "documind123"
    minio_bucket: str = "documents"
    minio_use_ssl: bool = False

    # Redis
    redis_addr: str = "localhost:6379"
    redis_password: str = ""
    redis_db: int = 0

    # Processing
    max_retries: int = 5
    idempotency_ttl_hours: int = 24

    # Embeddings
    embedding_model: str = "intfloat/multilingual-e5-large"
    embedding_device: str = "cuda"  # или "cpu"
    embedding_batch_size: int = 32

    # OCR
    tesseract_cmd: str = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    ocr_languages: str = "rus+eng"

    @property
    def db_dsn(self) -> str:
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


settings = Settings()