import math
from kafka.admin import KafkaAdminClient
import psycopg2
import redis

from .settings import settings


ANGLE_ITEM_ID = "S0000004"
ANGLE_CONT_PARAMETER = "angle_cont"


def create_redis_client() -> redis.Redis:
    return redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        password=settings.REDIS_PASSWORD or None,
        decode_responses=True,
    )


def create_postgres_connection():
    conn = psycopg2.connect(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        dbname=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
    )
    conn.autocommit = True
    return conn


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
                source TEXT NOT NULL,
                angle_rad DOUBLE PRECISION,
                angle_sin DOUBLE PRECISION,
                angle_cos DOUBLE PRECISION
            )
            """
        )
        cur.execute(
            """
            ALTER TABLE telemetry_history
            ADD COLUMN IF NOT EXISTS angle_rad DOUBLE PRECISION
            """
        )
        cur.execute(
            """
            ALTER TABLE telemetry_history
            ADD COLUMN IF NOT EXISTS angle_sin DOUBLE PRECISION
            """
        )
        cur.execute(
            """
            ALTER TABLE telemetry_history
            ADD COLUMN IF NOT EXISTS angle_cos DOUBLE PRECISION
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
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS subscriptions (
                id BIGSERIAL PRIMARY KEY,
                email TEXT NOT NULL,
                item_id TEXT,
                anomaly_type TEXT,
                enabled BOOLEAN NOT NULL DEFAULT TRUE,
                is_verified BOOLEAN NOT NULL DEFAULT FALSE,
                verify_token TEXT,
                unsubscribe_token TEXT NOT NULL,
                cooldown_minutes INTEGER,
                last_sent_at TIMESTAMPTZ,
                verified_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            ALTER TABLE subscriptions
            ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE
            """
        )
        cur.execute(
            """
            ALTER TABLE subscriptions
            ADD COLUMN IF NOT EXISTS is_verified BOOLEAN NOT NULL DEFAULT FALSE
            """
        )
        cur.execute(
            """
            ALTER TABLE subscriptions
            ADD COLUMN IF NOT EXISTS verify_token TEXT
            """
        )
        cur.execute(
            """
            ALTER TABLE subscriptions
            ADD COLUMN IF NOT EXISTS unsubscribe_token TEXT
            """
        )
        cur.execute(
            """
            ALTER TABLE subscriptions
            ADD COLUMN IF NOT EXISTS cooldown_minutes INTEGER
            """
        )
        cur.execute(
            """
            ALTER TABLE subscriptions
            ADD COLUMN IF NOT EXISTS last_sent_at TIMESTAMPTZ
            """
        )
        cur.execute(
            """
            ALTER TABLE subscriptions
            ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ
            """
        )
        cur.execute(
            """
            ALTER TABLE subscriptions
            ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            """
        )
        cur.execute(
            """
            UPDATE subscriptions
            SET unsubscribe_token = COALESCE(unsubscribe_token, md5(random()::text || clock_timestamp()::text))
            WHERE unsubscribe_token IS NULL
            """
        )
        cur.execute(
            """
            ALTER TABLE subscriptions
            ALTER COLUMN unsubscribe_token SET NOT NULL
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_subscriptions_enabled_verified
            ON subscriptions (enabled, is_verified)
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_subscriptions_verify_token
            ON subscriptions (verify_token)
            WHERE verify_token IS NOT NULL
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_subscriptions_unsubscribe_token
            ON subscriptions (unsubscribe_token)
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS notification_log (
                id BIGSERIAL PRIMARY KEY,
                subscription_id BIGINT NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
                anomaly_signature TEXT NOT NULL,
                item TEXT NOT NULL,
                anomaly_type TEXT NOT NULL,
                detected_at_utc TIMESTAMPTZ,
                sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                status TEXT NOT NULL,
                provider TEXT,
                provider_message_id TEXT,
                error_message TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_notification_log_subscription_signature
            ON notification_log (subscription_id, anomaly_signature)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notification_log_subscription_sent_at
            ON notification_log (subscription_id, sent_at DESC)
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS service_status (
                service_name TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                message TEXT,
                degraded_since TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS injection_jobs (
                id BIGSERIAL PRIMARY KEY,
                job_id TEXT NOT NULL UNIQUE,
                prototype_id TEXT NOT NULL,
                item_id TEXT NOT NULL,
                severity DOUBLE PRECISION NOT NULL,
                time_scale DOUBLE PRECISION NOT NULL,
                recenter BOOLEAN NOT NULL DEFAULT TRUE,
                status TEXT NOT NULL,
                requested_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                started_at_utc TIMESTAMPTZ,
                completed_at_utc TIMESTAMPTZ,
                failed_at_utc TIMESTAMPTZ,
                points_planned INTEGER,
                points_emitted INTEGER NOT NULL DEFAULT 0,
                requested_by_source TEXT NOT NULL DEFAULT 'api',
                feature_snapshot_json TEXT,
                error_message TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_injection_jobs_status_requested
            ON injection_jobs (status, requested_at_utc DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_injection_jobs_item_requested
            ON injection_jobs (item_id, requested_at_utc DESC)
            """
        )
    conn.commit()


def derive_angle_features(item: str, value_numeric: float | None):
    if item != ANGLE_ITEM_ID or value_numeric is None:
        return None, None, None

    angle_rad = value_numeric * math.pi / 180.0
    return angle_rad, math.sin(angle_rad), math.cos(angle_rad)


def insert_telemetry_history(conn, event: dict) -> None:
    angle_rad, angle_sin, angle_cos = derive_angle_features(
        event["item"],
        event.get("value_numeric"),
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO telemetry_history (
                item,
                received_at_utc,
                value_numeric,
                value_raw,
                source,
                angle_rad,
                angle_sin,
                angle_cos
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event["item"],
                event["received_at_utc"],
                event.get("value_numeric"),
                event.get("value_raw"),
                event["source"],
                angle_rad,
                angle_sin,
                angle_cos,
            ),
        )
    conn.commit()


def backfill_angle_features(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE telemetry_history
            SET angle_rad = value_numeric * pi() / 180.0,
                angle_sin = sin(value_numeric * pi() / 180.0),
                angle_cos = cos(value_numeric * pi() / 180.0)
            WHERE item = %s
              AND value_numeric IS NOT NULL
              AND (
                angle_rad IS NULL
                OR angle_sin IS NULL
                OR angle_cos IS NULL
              )
            """,
            (ANGLE_ITEM_ID,),
        )
        updated_rows = cur.rowcount
    conn.commit()
    return updated_rows


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


def upsert_service_status(
    conn,
    *,
    service_name: str,
    status: str,
    message: str,
    degraded_since=None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO service_status (
                service_name,
                status,
                message,
                degraded_since,
                updated_at
            )
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (service_name)
            DO UPDATE SET
                status = EXCLUDED.status,
                message = EXCLUDED.message,
                degraded_since = EXCLUDED.degraded_since,
                updated_at = NOW()
            """,
            (service_name, status, message, degraded_since),
        )


def get_service_status(conn, service_name: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT service_name, status, message, degraded_since, updated_at
            FROM service_status
            WHERE service_name = %s
            LIMIT 1
            """,
            (service_name,),
        )
        return cur.fetchone()


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
