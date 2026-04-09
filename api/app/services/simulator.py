import json
from datetime import datetime, timezone
from pathlib import Path
from time import time

from kafka import KafkaProducer

from config.runtime import create_redis_client

from ..config import settings
from ..schemas import SimulateAnomalyRequest


# Load rules from config file
RULES_PATH = Path(settings.RULES_FILE_PATH)

with open(RULES_PATH, "r") as f:
    rules = json.load(f)

THRESHOLD_RULES = rules["threshold_rules"]
JUMP_RULES = rules["jump_rules"]



KAFKA_BOOTSTRAP_SERVERS = settings.KAFKA_BOOTSTRAP_SERVERS
TELEMETRY_TOPIC = settings.KAFKA_TELEMETRY_TOPIC
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_unix_ms() -> int:
    return int(time() * 1000)


def get_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )


def get_latest_value_for_item(item: str):
    r = create_redis_client()
    raw = r.hget("latest_state", item)
    if raw is None:
        return None

    event = json.loads(raw)
    return event.get("value_numeric")


def build_simulated_value(item: str, mode: str) -> float:
    latest_value = get_latest_value_for_item(item)

    if mode == "threshold_breach_high":
        rule = THRESHOLD_RULES.get(item)
        if rule is None:
            raise ValueError(f"No threshold rule defined for item '{item}'")
        return rule["max"] + 100.0

    if mode == "threshold_breach_low":
        rule = THRESHOLD_RULES.get(item)
        if rule is None:
            raise ValueError(f"No threshold rule defined for item '{item}'")
        return rule["min"] - 100.0

    if mode == "sudden_jump":
        jump_threshold = JUMP_RULES.get(item)
        if jump_threshold is None:
            raise ValueError(f"No jump rule defined for item '{item}'")
        if latest_value is None:
            raise ValueError(f"No latest Redis value found for item '{item}'")
        return latest_value + (jump_threshold * 5)

    raise ValueError(
        f"Unsupported mode '{mode}'. "
        f"Use one of: threshold_breach_high, threshold_breach_low, sudden_jump"
    )


def build_simulated_event(item: str, value_numeric: float) -> dict:
    return {
        "received_at_utc": now_utc_iso(),
        "received_unix_ms": now_unix_ms(),
        "item": item,
        "value_raw": str(value_numeric),
        "value_numeric": value_numeric,
        "source_timestamp_raw": None,
        "source": "simulation_api",
    }


def publish_simulated_anomaly(request: SimulateAnomalyRequest) -> dict:
    value_numeric = build_simulated_value(request.item, request.mode)
    event = build_simulated_event(request.item, value_numeric)

    producer = get_kafka_producer()
    try:
        producer.send(TELEMETRY_TOPIC, key=request.item, value=event)
        producer.flush(timeout=10)
    finally:
        producer.close()

    return event
