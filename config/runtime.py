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
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS telemetry_history (
                id BIGSERIAL PRIMARY KEY,
                item TEXT NOT NULL,
                received_at_utc TIMESTAMPTZ NOT NULL,
                value_numeric DOUBLE PRECISION,
                value_raw TEXT,
                source TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_telemetry_history_item_received_at
            ON telemetry_history (item, received_at_utc DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_telemetry_history_received_at
            ON telemetry_history (received_at_utc DESC)
            """
        )
    conn.commit()


def insert_telemetry_history(conn, event: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO telemetry_history (
                item,
                received_at_utc,
                value_numeric,
                value_raw,
                source
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                event["item"],
                event["received_at_utc"],
                event.get("value_numeric"),
                event.get("value_raw"),
                event["source"],
            ),
        )
    conn.commit()


def cleanup_old_telemetry_history(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM telemetry_history
            WHERE received_at_utc < (NOW() - (%s * INTERVAL '1 day'))
            """,
            (settings.TELEMETRY_RETENTION_DAYS,),
        )
        deleted_rows = cur.rowcount
    conn.commit()
    return deleted_rows


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
