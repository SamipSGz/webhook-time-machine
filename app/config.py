from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    postgres_dsn: str = "postgresql://webhook:webhook@localhost:5432/webhook"

    nats_url: str = "nats://localhost:4222"
    delivery_subject: str = "deliveries.pending"
    delivery_stream: str = "DELIVERIES"
    delivery_durable: str = "delivery-worker"

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_db: str = "default"

    redis_url: str = "redis://localhost:6379/0"

    s3_endpoint: str = "http://localhost:9000"
    s3_region: str = "us-east-1"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_bucket: str = "webhook-payloads"

    max_attempts: int = 5
    base_backoff_seconds: int = 2
    delivery_timeout_seconds: int = 10

    public_base_url: str = "http://localhost:8000"


settings = Settings()
