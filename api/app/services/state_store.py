import json
from config.runtime import create_postgres_connection, create_redis_client

from ..schemas import TelemetryPoint, AnomalyEvent


def _redis_event_to_telemetry_point(raw_json: str) -> TelemetryPoint:
    event = json.loads(raw_json)
    return TelemetryPoint(
        item=event["item"],
        value=event.get("value_numeric"),
        timestamp_utc=event["received_at_utc"],
        source=event["source"],
    )


def _redis_recent_history_to_telemetry_point(raw_json: str) -> TelemetryPoint:
    event = json.loads(raw_json)
    return TelemetryPoint(
        item=event["item"],
        value=event.get("value"),
        timestamp_utc=event["timestamp_utc"],
        source=event["source"],
    )


def _row_to_telemetry_point(row) -> TelemetryPoint:
    return TelemetryPoint(
        item=row[0],
        value=row[1],
        timestamp_utc=row[2].isoformat(),
        source=row[3],
    )


def get_latest_telemetry():
    r = create_redis_client()
    raw_map = r.hgetall("latest_state")
    points = []

    for _, raw in sorted(raw_map.items()):
        points.append(_redis_event_to_telemetry_point(raw))

    return points


def get_latest_telemetry_by_item(item_id: str):
    r = create_redis_client()
    raw = r.hget("latest_state", item_id)
    if raw is None:
        return None
    return _redis_event_to_telemetry_point(raw)


def get_recent_telemetry_by_item(item_id: str, limit: int = 100):
    r = create_redis_client()
    raw_entries = r.lrange(f"recent_history:{item_id}", 0, limit - 1)
    points = [_redis_recent_history_to_telemetry_point(raw) for raw in raw_entries]
    return list(reversed(points))


def get_telemetry_history_by_item(item_id: str, from_utc, to_utc, limit: int = 1000):
    conn = create_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT item, value_numeric, received_at_utc, source
                FROM telemetry_history
                WHERE item = %s
                  AND received_at_utc >= %s
                  AND received_at_utc <= %s
                ORDER BY received_at_utc ASC
                LIMIT %s
                """,
                (item_id, from_utc, to_utc, limit),
            )
            rows = cur.fetchall()
        return [_row_to_telemetry_point(row) for row in rows]
    finally:
        conn.close()


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
        trigger_source=row[8],
        is_simulated=bool(row[9]),
    )


def get_recent_anomalies(limit: int = 50):
    conn = create_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT detected_at_utc, item, anomaly_type, value_numeric,
                       previous_value_numeric, threshold_value, details_json, source,
                       trigger_source, is_simulated
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
    conn = create_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT detected_at_utc, item, anomaly_type, value_numeric,
                       previous_value_numeric, threshold_value, details_json, source,
                       trigger_source, is_simulated
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
