import json
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

import psycopg2
import redis
from kafka import KafkaConsumer, KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TELEMETRY_TOPIC = "telemetry.raw"
ANOMALY_TOPIC = "anomaly.events"

REDIS_HOST = "localhost"
REDIS_PORT = 6379

POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DB = "iss_telemetry"
POSTGRES_USER = "iss_user"
POSTGRES_PASSWORD = "iss_password"


# Very simple first-pass rules.
# Tune later after observing real data longer.
THRESHOLD_RULES = {
    "USLAB000059": {"min": 22.0, "max": 25.0},      # Cabin temperature
    "NODE3000012": {"min": 16.5, "max": 18.0},      # Avionics cooling temp
    "NODE3000013": {"min": 3.8, "max": 4.8},        # Air cooling temp
    "P1000003": {"min": 3.5, "max": 5.0},           # Loop B PM out temp
    "S1000003": {"min": 3.5, "max": 5.0},           # Loop A PM out temp
    "NODE3000009": {"min": 40.0, "max": 50.0},      # Water tank
    "NODE3000005": {"min": 0.0, "max": 100.0},      # Urine tank %
    "S0000004": {"min": 0.0, "max": 360.0},         # SARJ angle
}

# Sudden jump thresholds by item.
JUMP_RULES = {
    "USLAB000059": 0.5,
    "NODE3000012": 0.3,
    "NODE3000013": 0.3,
    "P1000003": 0.4,
    "S1000003": 0.4,
    "NODE3000009": 0.2,
    "NODE3000005": 2.0,
    "S0000004": 10,
}

running = True


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_kafka_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TELEMETRY_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="iss-anomaly-worker-v1",
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


def create_redis_client() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )


def create_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def ensure_tables(conn) -> None:
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
                source TEXT NOT NULL
            )
            """
        )
    conn.commit()


def save_latest_state(r: redis.Redis, event: dict[str, Any]) -> None:
    item = event["item"]
    key = f"latest:{item}"
    r.set(key, json.dumps(event))


def build_anomaly_event(
    item: str,
    anomaly_type: str,
    value_numeric: Optional[float],
    previous_value_numeric: Optional[float],
    threshold_value: Optional[float],
    details: dict[str, Any],
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
                source
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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
            ),
        )
    conn.commit()


def detect_threshold_breach(item: str, value_numeric: Optional[float]) -> Optional[dict[str, Any]]:
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
        )

    if value_numeric > max_v:
        return build_anomaly_event(
            item=item,
            anomaly_type="threshold_breach_high",
            value_numeric=value_numeric,
            previous_value_numeric=None,
            threshold_value=max_v,
            details={"min_allowed": min_v, "max_allowed": max_v},
        )

    return None


def detect_sudden_jump(
    item: str,
    value_numeric: Optional[float],
    previous_value_numeric: Optional[float],
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
        )

    return None


def handle_shutdown(signum, frame):
    global running
    print(f"\n[shutdown] received signal {signum}, stopping worker...")
    running = False


def main() -> None:
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    print("[startup] connecting to Redis...")
    redis_client = create_redis_client()
    redis_client.ping()
    print("[startup] Redis connected")

    print("[startup] connecting to Postgres...")
    pg_conn = create_postgres_connection()
    ensure_tables(pg_conn)
    print("[startup] Postgres connected and tables ensured")

    print("[startup] creating Kafka producer...")
    producer = create_kafka_producer()

    print("[startup] creating Kafka consumer...")
    consumer = create_kafka_consumer()

    previous_values: dict[str, Optional[float]] = {}
    processed_count = 0
    anomaly_count = 0

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

                    save_latest_state(redis_client, event)

                    threshold_anomaly = detect_threshold_breach(item, value_numeric)
                    if threshold_anomaly is not None:
                        insert_anomaly(pg_conn, threshold_anomaly)
                        producer.send(ANOMALY_TOPIC, key=item, value=threshold_anomaly)
                        anomaly_count += 1
                        print(f"[anomaly] threshold breach: {threshold_anomaly}")

                    previous_value_numeric = previous_values.get(item)
                    jump_anomaly = detect_sudden_jump(item, value_numeric, previous_value_numeric)
                    if jump_anomaly is not None:
                        insert_anomaly(pg_conn, jump_anomaly)
                        producer.send(ANOMALY_TOPIC, key=item, value=jump_anomaly)
                        anomaly_count += 1
                        print(f"[anomaly] sudden jump: {jump_anomaly}")

                    previous_values[item] = value_numeric
                    processed_count += 1

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
  