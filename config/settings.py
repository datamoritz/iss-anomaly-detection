from functools import lru_cache
from urllib.parse import urlsplit, urlunsplit

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    APP_NAME: str = "ISS Telemetry API"
    APP_ENV: str = "dev"
    LOG_LEVEL: str = "INFO"

    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000

    LIGHTSTREAMER_SERVER: str = "https://push.lightstreamer.com"
    LIGHTSTREAMER_ADAPTER_SET: str = "ISSLIVE"

    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TELEMETRY_TOPIC: str = "telemetry.raw"
    KAFKA_ANOMALY_TOPIC: str = "anomaly.events"
    KAFKA_INJECTION_TOPIC: str = "injection.jobs"
    KAFKA_CONSUMER_GROUP: str = "iss-worker"
    KAFKA_NOTIFICATION_CONSUMER_GROUP: str = "iss-notifications"
    KAFKA_INJECTION_CONSUMER_GROUP: str = "iss-injection-worker"

    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str | None = None

    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "iss_telemetry"
    POSTGRES_USER: str = "iss_user"
    POSTGRES_PASSWORD: str = "iss_password"

    DATABASE_URL: str | None = None

    CORS_ALLOW_ORIGINS: str = "http://localhost:5173,http://127.0.0.1:5173"
    RULES_FILE_PATH: str = "config/rules.json"
    DATA_ROOT: str = "data"
    COLLECTOR_RAW_ROOT: str = "data/raw"
    COLLECTOR_MANIFEST_PATH: str = "data/manifest.json"
    REDIS_TELEMETRY_LIVE_CHANNEL: str = "telemetry:live"
    PROTOTYPE_LIBRARY_DIR: str = "data/anomaly_prototypes/smap_final9_v01"
    COLLECTOR_DIAGNOSTICS_PATH: str = "data/diagnostics/collector_gaps.jsonl"
    COLLECTOR_HEARTBEAT_WARN_SECONDS: int = 15
    COLLECTOR_HEARTBEAT_RECONNECT_SECONDS: int = 45
    COLLECTOR_GAP_THRESHOLD_SECONDS: int = 60
    APP_INGEST_DIAGNOSTICS_PATH: str = "data/diagnostics/ingest_gaps.jsonl"
    APP_INGEST_HEARTBEAT_WARN_SECONDS: int = 15
    APP_INGEST_HEARTBEAT_RECONNECT_SECONDS: int = 45
    APP_INGEST_GAP_THRESHOLD_SECONDS: int = 60
    REDIS_RECENT_HISTORY_LIMIT: int = 100
    REDIS_FEATURE_WINDOW_SIZE: int = 100
    INJECTION_MAX_POINTS: int = 500
    TELEMETRY_RETENTION_DAYS: int = 28
    WORKER_REDIS_RETRY_MAX_SECONDS: int = 60
    WORKER_REDIS_HEALTH_DEGRADED_AFTER_SECONDS: int = 300
    ANOMALY_POST_INJECTION_COOLDOWN_SECONDS: int = 10
    STARTUP_DEPENDENCY_RETRY_ATTEMPTS: int = 10
    STARTUP_DEPENDENCY_RETRY_DELAY_SECONDS: int = 3
    DEFAULT_NOTIFICATION_COOLDOWN_MINUTES: int = 10

    EMAIL_PROVIDER: str = "resend"
    RESEND_API_KEY: str | None = None
    EMAIL_FROM: str = "iss-alerts@example.com"
    APP_BASE_URL: str = "http://localhost:8000"

    @property
    def cors_origins(self) -> list[str]:
        return [
            origin.strip()
            for origin in self.CORS_ALLOW_ORIGINS.split(",")
            if origin.strip()
        ]

    @property
    def redis_url(self) -> str:
        if self.REDIS_PASSWORD:
            return (
                f"redis://:{self.REDIS_PASSWORD}@"
                f"{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
            )
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    @property
    def postgres_url(self) -> str:
        if self.DATABASE_URL:
            parsed = urlsplit(self.DATABASE_URL)
            scheme = parsed.scheme.replace("+psycopg", "")
            return urlunsplit(
                (scheme, parsed.netloc, parsed.path, parsed.query, parsed.fragment)
            )
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
