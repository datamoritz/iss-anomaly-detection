

import json
import redis

import psycopg2

from app.schemas import TelemetryPoint, AnomalyEvent


REDIS_HOST = "localhost"
REDIS_PORT = 6379

POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DB = "iss_telemetry"
POSTGRES_USER = "iss_user"
POSTGRES_PASSWORD = "iss_password"


def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def _redis_event_to_telemetry_point(raw_json: str) -> TelemetryPoint:
    event = json.loads(raw_json)
    return TelemetryPoint(
        item=event["item"],
        value=event.get("value_numeric"),
        timestamp_utc=event["received_at_utc"],
        source=event["source"],
    )


def get_latest_telemetry():
    r = get_redis_client()
    keys = sorted(r.keys("latest:*"))
    points = []

    for key in keys:
        raw = r.get(key)
        if raw is None:
            continue
        points.append(_redis_event_to_telemetry_point(raw))

    return points


def get_latest_telemetry_by_item(item_id: str):
    r = get_redis_client()
    raw = r.get(f"latest:{item_id}")
    if raw is None:
        return None
    return _redis_event_to_telemetry_point(raw)


def _row_to_anomaly_event(row) -> AnomalyEvent:
    return AnomalyEvent(
        detected_at_utc=row[0],
        item=row[1],
        anomaly_type=row[2],
        value_numeric=row[3],
        previous_value_numeric=row[4],
        threshold_value=row[5],
        details=json.loads(row[6]) if row[6] else {},
        source=row[7],
    )


def get_recent_anomalies(limit: int = 50):
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT detected_at_utc, item, anomaly_type, value_numeric,
                       previous_value_numeric, threshold_value, details_json, source
                FROM anomalies
                ORDER BY id DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
        return [_row_to_anomaly_event(row) for row in rows]
    finally:
        conn.close()


def get_recent_anomalies_by_item(item_id: str, limit: int = 50):
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT detected_at_utc, item, anomaly_type, value_numeric,
                       previous_value_numeric, threshold_value, details_json, source
                FROM anomalies
                WHERE item = %s
                ORDER BY id DESC
                LIMIT %s
                """,
                (item_id, limit),
            )
            rows = cur.fetchall()
        return [_row_to_anomaly_event(row) for row in rows]
    finally:
        conn.close()