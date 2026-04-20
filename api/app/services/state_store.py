import json
from config.runtime import (
    ANGLE_CONT_PARAMETER,
    ANGLE_ITEM_ID,
    create_postgres_connection,
    create_redis_client,
    derive_angle_features,
)

from ..schemas import TelemetryPoint, AnomalyEvent, ContinuousAnglePoint, FeatureState


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


def _redis_feature_state_to_model(raw_json: str) -> FeatureState:
    payload = json.loads(raw_json)
    return FeatureState(
        item=payload["item"],
        window_size=payload["window_size"],
        value_count=payload["value_count"],
        baseline_mean=payload.get("baseline_mean"),
        baseline_std=payload.get("baseline_std"),
        median_delta_t_seconds=payload.get("median_delta_t_seconds"),
        updated_at_utc=payload["updated_at_utc"],
        source=payload["source"],
    )


def _build_continuous_angle_point(
    *,
    angle_deg,
    timestamp_utc,
    source,
    angle_rad=None,
    angle_sin=None,
    angle_cos=None,
) -> ContinuousAnglePoint:
    if angle_rad is None or angle_sin is None or angle_cos is None:
        angle_rad, angle_sin, angle_cos = derive_angle_features(
            ANGLE_ITEM_ID,
            angle_deg,
        )

    return ContinuousAnglePoint(
        parameter=ANGLE_CONT_PARAMETER,
        item=ANGLE_ITEM_ID,
        angle_deg=angle_deg,
        angle_rad=angle_rad,
        angle_sin=angle_sin,
        angle_cos=angle_cos,
        timestamp_utc=timestamp_utc,
        source=source,
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


def get_feature_state_by_item(item_id: str):
    r = create_redis_client()
    raw = r.hget("feature_state", item_id)
    if raw is None:
        return None
    return _redis_feature_state_to_model(raw)


def get_latest_continuous_angle():
    r = create_redis_client()
    raw = r.hget("latest_state", ANGLE_ITEM_ID)
    if raw is None:
        return None

    event = json.loads(raw)
    return _build_continuous_angle_point(
        angle_deg=event.get("value_numeric"),
        timestamp_utc=event["received_at_utc"],
        source=event["source"],
    )


def get_recent_continuous_angle(limit: int = 100):
    r = create_redis_client()
    raw_entries = r.lrange(f"recent_history:{ANGLE_ITEM_ID}", 0, limit - 1)
    points = []
    for raw in reversed(raw_entries):
        event = json.loads(raw)
        points.append(
            _build_continuous_angle_point(
                angle_deg=event.get("value"),
                timestamp_utc=event["timestamp_utc"],
                source=event["source"],
            )
        )
    return points


def get_continuous_angle_history(from_utc, to_utc, limit: int = 1000):
    conn = create_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value_numeric, received_at_utc, source,
                       angle_rad, angle_sin, angle_cos
                FROM telemetry_history
                WHERE item = %s
                  AND received_at_utc >= %s
                  AND received_at_utc <= %s
                ORDER BY received_at_utc ASC
                LIMIT %s
                """,
                (ANGLE_ITEM_ID, from_utc, to_utc, limit),
            )
            rows = cur.fetchall()
        return [
            _build_continuous_angle_point(
                angle_deg=row[0],
                timestamp_utc=row[1].isoformat(),
                source=row[2],
                angle_rad=row[3],
                angle_sin=row[4],
                angle_cos=row[5],
            )
            for row in rows
        ]
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
