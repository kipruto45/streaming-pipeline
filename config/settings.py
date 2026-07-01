from pydantic import BaseSettings

class Settings(BaseSettings):
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    SCHEMA_REGISTRY_URL: str = "http://localhost:8081"

    # Database Configuration
    POSTGRES_DSN: str = "postgresql://user:pw@localhost/db"
    REDIS_URL: str = "redis://localhost:6379"

    # Flink Configuration
    FLINK_PARALLELISM: int = 4
    CHECKPOINT_INTERVAL_MS: int = 60000

    # Application Logic
    ANOMALY_ZSCORE_THRESHOLD: float = 3.0
    WINDOW_SIZE_SECONDS: int = 60
    DLQ_TOPIC: str = "dlq.failed.events"

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }

settings = Settings()
