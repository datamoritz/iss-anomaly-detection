from kafka.admin import KafkaAdminClient
import psycopg2
import redis

from .settings import settings


def create_redis_client() -> redis.Redis:
    return redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        password=settings.REDIS_PASSWORD or None,
        decode_responses=True,
    )


def create_postgres_connection():
    return psycopg2.connect(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        dbname=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
    )


def ensure_postgres_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS anomalies (
                id SERIAL PRIMARY KEY,
                detected_at_utc TEXT NOT NULL,
                item TEXT NOT NULL,
                anomaly_type TEXT NOT NULL,
                value_numeric DOUBLE PRECISION,
                previous_value_numeric DOUBLE PRECISION,
                threshold_value DOUBLE PRECISION,
                details_json TEXT NOT NULL,
                source TEXT NOT NULL,
                trigger_source TEXT,
                is_simulated BOOLEAN NOT NULL DEFAULT FALSE
            )
            """
        )
        cur.execute(
            """
            ALTER TABLE anomalies
            ADD COLUMN IF NOT EXISTS trigger_source TEXT
            """
        )
        cur.execute(
            """
            ALTER TABLE anomalies
            ADD COLUMN IF NOT EXISTS is_simulated BOOLEAN NOT NULL DEFAULT FALSE
            """
        )
    conn.commit()


def check_redis() -> tuple[bool, str]:
    try:
        client = create_redis_client()
        client.ping()
        return True, "ok"
    except Exception as exc:
        return False, str(exc)


def check_postgres() -> tuple[bool, str]:
    conn = None
    try:
        conn = create_postgres_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return True, "ok"
    except Exception as exc:
        return False, str(exc)
    finally:
        if conn is not None:
            conn.close()


def check_kafka() -> tuple[bool, str]:
    client = None
    try:
        client = KafkaAdminClient(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=3000,
            api_version_auto_timeout_ms=3000,
        )
        client.list_topics()
        return True, "ok"
    except Exception as exc:
        return False, str(exc)
    finally:
        if client is not None:
            client.close()
