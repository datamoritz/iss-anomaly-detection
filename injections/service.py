import csv
import json
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from time import time as unix_time

import numpy as np
from kafka import KafkaConsumer, KafkaProducer

from config.runtime import create_postgres_connection, ensure_postgres_schema
from config.settings import settings


KAFKA_BOOTSTRAP_SERVERS = settings.KAFKA_BOOTSTRAP_SERVERS
INJECTION_TOPIC = settings.KAFKA_INJECTION_TOPIC
TELEMETRY_TOPIC = settings.KAFKA_TELEMETRY_TOPIC
PROTOTYPE_LIBRARY_DIR = Path(settings.PROTOTYPE_LIBRARY_DIR)
PROTOTYPE_MANIFEST_PATH = PROTOTYPE_LIBRARY_DIR / "prototype_manifest.csv"
PROTOTYPE_ARRAYS_PATH = PROTOTYPE_LIBRARY_DIR / "prototype_arrays.npz"
MAX_POINTS = settings.INJECTION_MAX_POINTS

running = True


@dataclass
class ExecutionState:
    job_id: str
    prototype_id: str
    item_id: str
    severity: float
    time_scale: float
    recenter: bool
    baseline_mean: float
    baseline_std: float
    median_delta_t_seconds: float
    shape_resampled: np.ndarray

    @property
    def points_planned(self) -> int:
        return int(self.shape_resampled.shape[0])


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_utc_iso() -> str:
    return now_utc().isoformat()


def now_unix_ms() -> int:
    return int(unix_time() * 1000)


@lru_cache
def load_prototype_manifest() -> dict[str, dict]:
    with PROTOTYPE_MANIFEST_PATH.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return {row["prototype_id"]: row for row in rows}


@lru_cache
def load_prototype_arrays() -> dict[str, np.ndarray]:
    arrays = np.load(PROTOTYPE_ARRAYS_PATH, allow_pickle=False)
    return {prototype_id: arrays[prototype_id].astype(np.float64) for prototype_id in arrays.files}


def create_kafka_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        INJECTION_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        group_id=settings.KAFKA_INJECTION_CONSUMER_GROUP,
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


def handle_shutdown(signum, frame):
    global running
    print(f"\n[shutdown] received signal {signum}, stopping injection worker...")
    running = False


def resample_sequence(sequence: np.ndarray, target_points: int) -> np.ndarray:
    if target_points <= 1:
        return np.array([float(sequence[0])], dtype=np.float64)
    if sequence.shape[0] == target_points:
        return sequence.astype(np.float64, copy=True)

    original_x = np.linspace(0.0, 1.0, num=sequence.shape[0])
    target_x = np.linspace(0.0, 1.0, num=target_points)
    return np.interp(target_x, original_x, sequence).astype(np.float64)


def build_execution_state(job_event: dict) -> ExecutionState:
    prototype_id = job_event["prototype_id"]
    arrays = load_prototype_arrays()
    if prototype_id not in arrays:
        raise ValueError(f"Prototype '{prototype_id}' not found in arrays")

    feature_snapshot = job_event.get("feature_snapshot") or {}
    baseline_mean = feature_snapshot.get("baseline_mean")
    baseline_std = feature_snapshot.get("baseline_std")
    median_delta_t_seconds = feature_snapshot.get("median_delta_t_seconds")

    if baseline_mean is None or baseline_std is None or baseline_std <= 0:
        raise ValueError("Feature snapshot is missing a usable baseline mean/std")
    if median_delta_t_seconds is None or median_delta_t_seconds <= 0:
        raise ValueError("Feature snapshot is missing a usable cadence")

    raw_shape = arrays[prototype_id].astype(np.float64, copy=True)
    if raw_shape.size == 0:
        raise ValueError(f"Prototype '{prototype_id}' is empty")

    if job_event.get("recenter", True):
        raw_shape = raw_shape - float(np.mean(raw_shape))

    target_points = max(1, int(round(raw_shape.shape[0] * job_event["time_scale"])))
    target_points = min(target_points, MAX_POINTS)
    shape_resampled = resample_sequence(raw_shape, target_points)

    return ExecutionState(
        job_id=job_event["job_id"],
        prototype_id=prototype_id,
        item_id=job_event["item_id"],
        severity=float(job_event["severity"]),
        time_scale=float(job_event["time_scale"]),
        recenter=bool(job_event.get("recenter", True)),
        baseline_mean=float(baseline_mean),
        baseline_std=float(baseline_std),
        median_delta_t_seconds=float(median_delta_t_seconds),
        shape_resampled=shape_resampled,
    )


def build_injected_event(state: ExecutionState, point_index: int, z_value: float) -> dict:
    value_numeric = state.baseline_mean + (z_value * state.baseline_std * state.severity)
    return {
        "received_at_utc": now_utc_iso(),
        "received_unix_ms": now_unix_ms(),
        "item": state.item_id,
        "value_raw": str(value_numeric),
        "value_numeric": value_numeric,
        "source_timestamp_raw": None,
        "source": "prototype_injection_worker",
        "is_injected": True,
        "injection_job_id": state.job_id,
        "prototype_id": state.prototype_id,
        "point_index": point_index,
        "points_planned": state.points_planned,
    }


def mark_job_started(conn, state: ExecutionState) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE injection_jobs
            SET status = %s,
                started_at_utc = %s,
                points_planned = %s,
                error_message = NULL
            WHERE job_id = %s
            """,
            ("running", now_utc(), state.points_planned, state.job_id),
        )


def mark_job_progress(conn, *, job_id: str, points_emitted: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE injection_jobs
            SET points_emitted = %s
            WHERE job_id = %s
            """,
            (points_emitted, job_id),
        )


def mark_job_completed(conn, *, job_id: str, points_emitted: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE injection_jobs
            SET status = %s,
                completed_at_utc = %s,
                points_emitted = %s
            WHERE job_id = %s
            """,
            ("completed", now_utc(), points_emitted, job_id),
        )


def mark_job_failed(conn, *, job_id: str, points_emitted: int, error_message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE injection_jobs
            SET status = %s,
                failed_at_utc = %s,
                points_emitted = %s,
                error_message = %s
            WHERE job_id = %s
            """,
            ("failed", now_utc(), points_emitted, error_message, job_id),
        )


def execute_job(conn, producer: KafkaProducer, job_event: dict) -> None:
    points_emitted = 0
    job_id = str(job_event.get("job_id", "unknown"))

    try:
        state = build_execution_state(job_event)
        job_id = state.job_id
        mark_job_started(conn, state)

        for point_index, z_value in enumerate(state.shape_resampled):
            if not running:
                raise RuntimeError("Injection worker interrupted during playback")

            event = build_injected_event(state, point_index, float(z_value))
            producer.send(TELEMETRY_TOPIC, key=state.item_id, value=event)
            producer.flush(timeout=10)
            points_emitted = point_index + 1
            mark_job_progress(conn, job_id=state.job_id, points_emitted=points_emitted)

            if point_index < state.points_planned - 1:
                time.sleep(state.median_delta_t_seconds)

        mark_job_completed(conn, job_id=state.job_id, points_emitted=points_emitted)
        print(
            "[injection] completed "
            f"job_id={state.job_id} prototype={state.prototype_id} "
            f"item={state.item_id} points={points_emitted}"
        )
    except Exception as exc:
        mark_job_failed(
            conn,
            job_id=job_id,
            points_emitted=points_emitted,
            error_message=str(exc),
        )
        print(f"[injection] failed job_id={job_id}: {exc}")


def main() -> None:
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    print("[startup] connecting to Postgres...")
    pg_conn = create_postgres_connection()
    ensure_postgres_schema(pg_conn)
    print("[startup] Postgres connected")

    print("[startup] creating Kafka producer...")
    producer = create_kafka_producer()

    print("[startup] creating Kafka consumer...")
    consumer = create_kafka_consumer()

    print("[startup] injection worker is running")
    print(f"[startup] consuming topic={INJECTION_TOPIC}")
    print(f"[startup] publishing injected telemetry to topic={TELEMETRY_TOPIC}")

    try:
        while running:
            records = consumer.poll(timeout_ms=1000)
            for _, messages in records.items():
                for message in messages:
                    execute_job(pg_conn, producer, message.value)
                    consumer.commit()
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
