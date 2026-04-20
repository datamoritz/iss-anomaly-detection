import json
import signal
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from config.settings import settings
from config.runtime import (
    cleanup_old_telemetry_history,
    create_postgres_connection,
    create_redis_client,
    ensure_postgres_schema,
    insert_telemetry_history,
    upsert_service_status,
)

import redis
from kafka import KafkaConsumer, KafkaProducer

# Load rules from config file
RULES_PATH = Path(settings.RULES_FILE_PATH)

with open(RULES_PATH, "r") as f:
    rules = json.load(f)

THRESHOLD_RULES = rules["threshold_rules"]
JUMP_RULES = rules["jump_rules"]


KAFKA_BOOTSTRAP_SERVERS = settings.KAFKA_BOOTSTRAP_SERVERS
TELEMETRY_TOPIC = settings.KAFKA_TELEMETRY_TOPIC
ANOMALY_TOPIC = settings.KAFKA_ANOMALY_TOPIC
REDIS_RECENT_HISTORY_LIMIT = settings.REDIS_RECENT_HISTORY_LIMIT
REDIS_FEATURE_WINDOW_SIZE = settings.REDIS_FEATURE_WINDOW_SIZE
WORKER_STATUS_SERVICE_NAME = "worker_redis_cache"

running = True


@dataclass
class RedisWriteState:
    degraded_since: datetime | None = None
    next_retry_at: float = 0.0
    retry_delay_seconds: int = 1

    @property
    def is_degraded(self) -> bool:
        return self.degraded_since is not None


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_kafka_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TELEMETRY_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=settings.KAFKA_CONSUMER_GROUP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )


def create_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )


def save_latest_state(r: redis.Redis, event: dict[str, Any]) -> None:
    item = event["item"]
    r.hset("latest_state", item, json.dumps(event))


def append_recent_history(r: redis.Redis, event: dict[str, Any]) -> None:
    item = event["item"]
    history_key = f"recent_history:{item}"
    history_entry = {
        "item": item,
        "value": event.get("value_numeric"),
        "value_raw": event.get("value_raw"),
        "timestamp_utc": event["received_at_utc"],
        "source": event["source"],
    }
    pipe = r.pipeline()
    pipe.lpush(history_key, json.dumps(history_entry))
    pipe.ltrim(history_key, 0, REDIS_RECENT_HISTORY_LIMIT - 1)
    pipe.execute()


def append_normal_feature_history(r: redis.Redis, event: dict[str, Any]) -> None:
    item = event["item"]
    history_key = f"normal_history:{item}"
    history_entry = {
        "item": item,
        "value": event.get("value_numeric"),
        "timestamp_utc": event["received_at_utc"],
        "source": event["source"],
    }
    pipe = r.pipeline()
    pipe.lpush(history_key, json.dumps(history_entry))
    pipe.ltrim(history_key, 0, REDIS_FEATURE_WINDOW_SIZE - 1)
    pipe.execute()


def _load_normal_history(r: redis.Redis, item: str) -> list[dict[str, Any]]:
    raw_entries = r.lrange(f"normal_history:{item}", 0, REDIS_FEATURE_WINDOW_SIZE - 1)
    return [json.loads(raw) for raw in reversed(raw_entries)]


def _compute_feature_state(item: str, history: list[dict[str, Any]]) -> dict[str, Any]:
    values = [entry["value"] for entry in history if entry.get("value") is not None]
    timestamps = []
    for entry in history:
        timestamp = entry.get("timestamp_utc")
        if not timestamp:
            continue
        try:
            timestamps.append(datetime.fromisoformat(timestamp))
        except ValueError:
            continue

    delta_seconds = [
        (current - previous).total_seconds()
        for previous, current in zip(timestamps, timestamps[1:])
        if current >= previous
    ]

    mean_value = statistics.fmean(values) if values else None
    std_value = statistics.stdev(values) if len(values) >= 2 else 0.0 if values else None
    median_delta_seconds = (
        statistics.median(delta_seconds) if delta_seconds else None
    )

    return {
        "item": item,
        "window_size": len(history),
        "value_count": len(values),
        "baseline_mean": mean_value,
        "baseline_std": std_value,
        "median_delta_t_seconds": median_delta_seconds,
        "updated_at_utc": now_utc_iso(),
        "source": "normal_only_feature_window",
    }


def update_feature_state(r: redis.Redis, event: dict[str, Any]) -> None:
    item = event["item"]
    append_normal_feature_history(r, event)
    history = _load_normal_history(r, item)
    feature_state = _compute_feature_state(item, history)
    r.hset("feature_state", item, json.dumps(feature_state))


def should_update_latest_state(event: dict[str, Any]) -> bool:
    # Keep Redis latest_state reserved for real telemetry so simulation
    # does not leave the chart stuck until the next collector sample arrives.
    return event.get("source") != "simulation_api"


def should_update_feature_state(
    event: dict[str, Any],
    *,
    threshold_anomaly: Optional[dict[str, Any]],
    jump_anomaly: Optional[dict[str, Any]],
) -> bool:
    if event.get("source") in {"simulation_api", "prototype_injection_worker"}:
        return False
    if event.get("is_injected"):
        return False
    if event.get("value_numeric") is None:
        return False
    if threshold_anomaly is not None or jump_anomaly is not None:
        return False
    return True


def reconnect_redis_client(previous_client: redis.Redis | None) -> redis.Redis:
    if previous_client is not None:
        try:
            previous_client.close()
        except Exception:
            pass

    print("[redis] reconnecting client...")
    client = create_redis_client()
    client.ping()
    print("[redis] reconnected")
    return client


def mark_redis_cache_status(
    conn,
    *,
    state: RedisWriteState,
    message: str,
    force: bool = False,
) -> None:
    if not force and not state.is_degraded:
        return
    status = "degraded" if state.is_degraded else "ok"
    upsert_service_status(
        conn,
        service_name=WORKER_STATUS_SERVICE_NAME,
        status=status,
        message=message,
        degraded_since=state.degraded_since,
    )
    conn.commit()


def write_event_to_redis(
    redis_client: redis.Redis,
    event: dict[str, Any],
    *,
    state: RedisWriteState,
    pg_conn,
) -> redis.Redis:
    should_write_latest = should_update_latest_state(event)

    now_monotonic = time.monotonic()
    if state.is_degraded and now_monotonic < state.next_retry_at:
        return redis_client

    attempts = 2 if not state.is_degraded else 1

    for attempt in range(attempts):
        try:
            if state.is_degraded:
                redis_client = reconnect_redis_client(redis_client)

            append_recent_history(redis_client, event)
            if should_write_latest:
                save_latest_state(redis_client, event)

            if state.is_degraded:
                print("[redis] cache writes recovered")
                state.degraded_since = None
                state.next_retry_at = 0.0
                state.retry_delay_seconds = 1
                mark_redis_cache_status(
                    pg_conn,
                    state=state,
                    message="Redis cache writes healthy",
                    force=True,
                )
            return redis_client
        except redis.exceptions.RedisError as exc:
            print(
                "[redis] write failed "
                f"(attempt {attempt + 1}/{attempts}): {exc}"
            )
            if not state.is_degraded:
                state.degraded_since = datetime.now(timezone.utc)

            state.next_retry_at = time.monotonic() + state.retry_delay_seconds
            state.retry_delay_seconds = min(
                state.retry_delay_seconds * 2,
                settings.WORKER_REDIS_RETRY_MAX_SECONDS,
            )
            mark_redis_cache_status(
                pg_conn,
                state=state,
                message=(
                    f"Redis cache writes failing: {exc}. "
                    f"Retrying in {state.retry_delay_seconds} seconds."
                ),
                force=True,
            )
            if attempt < attempts - 1:
                time.sleep(1)

    return redis_client


def build_anomaly_event(
    item: str,
    anomaly_type: str,
    value_numeric: Optional[float],
    previous_value_numeric: Optional[float],
    threshold_value: Optional[float],
    details: dict[str, Any],
    trigger_source: Optional[str],
) -> dict[str, Any]:
    return {
        "detected_at_utc": now_utc_iso(),
        "item": item,
        "anomaly_type": anomaly_type,
        "value_numeric": value_numeric,
        "previous_value_numeric": previous_value_numeric,
        "threshold_value": threshold_value,
        "details": details,
        "source": "threshold_worker_v1",
        "trigger_source": trigger_source,
        "is_simulated": trigger_source == "simulation_api",
    }


def insert_anomaly(conn, anomaly_event: dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO anomalies (
                detected_at_utc,
                item,
                anomaly_type,
                value_numeric,
                previous_value_numeric,
                threshold_value,
                details_json,
                source,
                trigger_source,
                is_simulated
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                anomaly_event["detected_at_utc"],
                anomaly_event["item"],
                anomaly_event["anomaly_type"],
                anomaly_event["value_numeric"],
                anomaly_event["previous_value_numeric"],
                anomaly_event["threshold_value"],
                json.dumps(anomaly_event["details"]),
                anomaly_event["source"],
                anomaly_event["trigger_source"],
                anomaly_event["is_simulated"],
            ),
        )
    conn.commit()


def detect_threshold_breach(
    item: str,
    value_numeric: Optional[float],
    trigger_source: Optional[str],
) -> Optional[dict[str, Any]]:
    if value_numeric is None:
        return None

    rule = THRESHOLD_RULES.get(item)
    if not rule:
        return None

    min_v = rule["min"]
    max_v = rule["max"]

    if value_numeric < min_v:
        return build_anomaly_event(
            item=item,
            anomaly_type="threshold_breach_low",
            value_numeric=value_numeric,
            previous_value_numeric=None,
            threshold_value=min_v,
            details={"min_allowed": min_v, "max_allowed": max_v},
            trigger_source=trigger_source,
        )

    if value_numeric > max_v:
        return build_anomaly_event(
            item=item,
            anomaly_type="threshold_breach_high",
            value_numeric=value_numeric,
            previous_value_numeric=None,
            threshold_value=max_v,
            details={"min_allowed": min_v, "max_allowed": max_v},
            trigger_source=trigger_source,
        )

    return None


def detect_sudden_jump(
    item: str,
    value_numeric: Optional[float],
    previous_value_numeric: Optional[float],
    trigger_source: Optional[str],
) -> Optional[dict[str, Any]]:
    if value_numeric is None or previous_value_numeric is None:
        return None

    jump_threshold = JUMP_RULES.get(item)
    if jump_threshold is None:
        return None

    delta = abs(value_numeric - previous_value_numeric)
    if delta > jump_threshold:
        return build_anomaly_event(
            item=item,
            anomaly_type="sudden_jump",
            value_numeric=value_numeric,
            previous_value_numeric=previous_value_numeric,
            threshold_value=jump_threshold,
            details={"delta": delta},
            trigger_source=trigger_source,
        )

    return None


def handle_shutdown(signum, frame):
    global running
    print(f"\n[shutdown] received signal {signum}, stopping worker...")
    running = False


def maybe_run_daily_retention_cleanup(conn, last_cleanup_day):
    today_utc = datetime.now(timezone.utc).date()
    if last_cleanup_day == today_utc:
        return last_cleanup_day

    deleted_rows = cleanup_old_telemetry_history(conn)
    print(
        "[retention] "
        f"removed {deleted_rows} telemetry rows older than "
        f"{settings.TELEMETRY_RETENTION_DAYS} days"
    )
    return today_utc


def main() -> None:
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    print("[startup] connecting to Redis...")
    redis_client = create_redis_client()
    redis_client.ping()
    print("[startup] Redis connected")

    print("[startup] connecting to Postgres...")
    pg_conn = create_postgres_connection()
    ensure_postgres_schema(pg_conn)
    print("[startup] Postgres connected and tables ensured")

    print("[startup] creating Kafka producer...")
    producer = create_kafka_producer()

    print("[startup] creating Kafka consumer...")
    consumer = create_kafka_consumer()

    previous_values: dict[str, Optional[float]] = {}
    processed_count = 0
    anomaly_count = 0
    last_cleanup_day = None
    redis_state = RedisWriteState()
    mark_redis_cache_status(
        pg_conn,
        state=redis_state,
        message="Redis cache writes healthy",
        force=True,
    )

    print("[startup] worker is running")
    print(f"[startup] consuming topic={TELEMETRY_TOPIC}")
    print(f"[startup] publishing anomalies to topic={ANOMALY_TOPIC}")

    try:
        while running:
            records = consumer.poll(timeout_ms=1000)
            for _, messages in records.items():
                for message in messages:
                    event = message.value
                    item = event["item"]
                    value_numeric = event.get("value_numeric")
                    trigger_source = event.get("source")

                    insert_telemetry_history(pg_conn, event)
                    redis_client = write_event_to_redis(
                        redis_client,
                        event,
                        state=redis_state,
                        pg_conn=pg_conn,
                    )

                    threshold_anomaly = detect_threshold_breach(
                        item,
                        value_numeric,
                        trigger_source,
                    )
                    if threshold_anomaly is not None:
                        insert_anomaly(pg_conn, threshold_anomaly)
                        producer.send(ANOMALY_TOPIC, key=item, value=threshold_anomaly)
                        anomaly_count += 1
                        print(f"[anomaly] threshold breach: {threshold_anomaly}")

                    previous_value_numeric = previous_values.get(item)
                    jump_anomaly = detect_sudden_jump(
                        item,
                        value_numeric,
                        previous_value_numeric,
                        trigger_source,
                    )
                    if jump_anomaly is not None:
                        insert_anomaly(pg_conn, jump_anomaly)
                        producer.send(ANOMALY_TOPIC, key=item, value=jump_anomaly)
                        anomaly_count += 1
                        print(f"[anomaly] sudden jump: {jump_anomaly}")

                    if should_update_feature_state(
                        event,
                        threshold_anomaly=threshold_anomaly,
                        jump_anomaly=jump_anomaly,
                    ):
                        try:
                            update_feature_state(redis_client, event)
                        except redis.exceptions.RedisError as exc:
                            print(f"[redis] feature state update failed: {exc}")

                    previous_values[item] = value_numeric
                    processed_count += 1
                    last_cleanup_day = maybe_run_daily_retention_cleanup(
                        pg_conn,
                        last_cleanup_day,
                    )

                    if processed_count % 100 == 0:
                        print(
                            f"[worker] processed={processed_count} "
                            f"anomalies={anomaly_count}"
                        )

    finally:
        print("[shutdown] closing Kafka consumer...")
        try:
            consumer.close()
        except Exception:
            pass

        print("[shutdown] flushing Kafka producer...")
        try:
            producer.flush(timeout=10)
            producer.close()
        except Exception:
            pass

        print("[shutdown] closing Postgres...")
        try:
            pg_conn.close()
        except Exception:
            pass

        print("[shutdown] done")


if __name__ == "__main__":
    main()
  
