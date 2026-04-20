import csv
import json
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from time import time
from uuid import uuid4

import numpy as np
from fastapi import HTTPException
from kafka import KafkaProducer

from config.runtime import create_postgres_connection, create_redis_client

from ..config import settings
from ..schemas import InjectionJobCreateRequest


PROTOTYPE_LIBRARY_DIR = Path(settings.PROTOTYPE_LIBRARY_DIR)
PROTOTYPE_MANIFEST_PATH = PROTOTYPE_LIBRARY_DIR / "prototype_manifest.csv"
PROTOTYPE_ARRAYS_PATH = PROTOTYPE_LIBRARY_DIR / "prototype_arrays.npz"
KAFKA_BOOTSTRAP_SERVERS = settings.KAFKA_BOOTSTRAP_SERVERS
INJECTION_TOPIC = settings.KAFKA_INJECTION_TOPIC


def now_utc():
    return datetime.now(timezone.utc)


def now_utc_iso() -> str:
    return now_utc().isoformat()


def now_unix_ms() -> int:
    return int(time() * 1000)


@lru_cache
def load_prototype_manifest() -> dict[str, dict]:
    if not PROTOTYPE_MANIFEST_PATH.exists():
        raise RuntimeError(f"Prototype manifest not found: {PROTOTYPE_MANIFEST_PATH}")
    with PROTOTYPE_MANIFEST_PATH.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return {row["prototype_id"]: row for row in rows}


@lru_cache
def load_prototype_lengths() -> dict[str, int]:
    if not PROTOTYPE_ARRAYS_PATH.exists():
        raise RuntimeError(f"Prototype arrays not found: {PROTOTYPE_ARRAYS_PATH}")
    arrays = np.load(PROTOTYPE_ARRAYS_PATH, allow_pickle=False)
    return {prototype_id: int(arrays[prototype_id].shape[0]) for prototype_id in arrays.files}


def get_feature_snapshot(item_id: str) -> dict | None:
    redis_client = create_redis_client()
    raw = redis_client.hget("feature_state", item_id)
    if raw is None:
        return None
    return json.loads(raw)


def create_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )


def insert_injection_job(
    conn,
    *,
    job_id: str,
    request: InjectionJobCreateRequest,
    points_planned: int,
    feature_snapshot: dict | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO injection_jobs (
                job_id,
                prototype_id,
                item_id,
                severity,
                time_scale,
                recenter,
                status,
                requested_at_utc,
                points_planned,
                requested_by_source,
                feature_snapshot_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                job_id,
                request.prototype_id,
                request.item_id,
                request.severity,
                request.time_scale,
                request.recenter,
                "queued",
                now_utc(),
                points_planned,
                "api",
                json.dumps(feature_snapshot) if feature_snapshot is not None else None,
            ),
        )


def mark_injection_job_failed(conn, *, job_id: str, error_message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE injection_jobs
            SET status = %s,
                failed_at_utc = %s,
                error_message = %s
            WHERE job_id = %s
            """,
            ("failed_to_publish", now_utc(), error_message, job_id),
        )


def build_injection_job_event(
    *,
    job_id: str,
    request: InjectionJobCreateRequest,
    points_planned: int,
    feature_snapshot: dict | None,
) -> dict:
    return {
        "job_id": job_id,
        "requested_at_utc": now_utc_iso(),
        "requested_unix_ms": now_unix_ms(),
        "prototype_id": request.prototype_id,
        "item_id": request.item_id,
        "severity": request.severity,
        "time_scale": request.time_scale,
        "recenter": request.recenter,
        "points_planned": points_planned,
        "feature_snapshot": feature_snapshot,
        "source": "injection_api_v1",
    }


def create_injection_job(request: InjectionJobCreateRequest) -> dict:
    if request.severity <= 0:
        raise HTTPException(status_code=400, detail="severity must be > 0")
    if request.time_scale <= 0:
        raise HTTPException(status_code=400, detail="time_scale must be > 0")

    manifest = load_prototype_manifest()
    lengths = load_prototype_lengths()

    if request.prototype_id not in manifest or request.prototype_id not in lengths:
        raise HTTPException(
            status_code=404,
            detail=f"Prototype '{request.prototype_id}' not found",
        )

    feature_snapshot = get_feature_snapshot(request.item_id)
    if feature_snapshot is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No rolling feature state found for telemetry item '{request.item_id}'. "
                "Wait for enough normal history to accumulate first."
            ),
        )

    points_planned = lengths[request.prototype_id]
    job_id = str(uuid4())
    job_event = build_injection_job_event(
        job_id=job_id,
        request=request,
        points_planned=points_planned,
        feature_snapshot=feature_snapshot,
    )

    conn = create_postgres_connection()
    insert_injection_job(
        conn,
        job_id=job_id,
        request=request,
        points_planned=points_planned,
        feature_snapshot=feature_snapshot,
    )

    producer = create_kafka_producer()
    try:
        producer.send(INJECTION_TOPIC, key=request.item_id, value=job_event)
        producer.flush(timeout=10)
    except Exception as exc:
        mark_injection_job_failed(conn, job_id=job_id, error_message=str(exc))
        raise HTTPException(status_code=500, detail=f"Injection job publish failed: {exc}")
    finally:
        producer.close()
        conn.close()

    return {
        "ok": True,
        "message": "Injection job queued",
        "job_id": job_id,
        "status": "queued",
        "prototype_id": request.prototype_id,
        "item_id": request.item_id,
        "severity": request.severity,
        "time_scale": request.time_scale,
        "recenter": request.recenter,
        "points_planned": points_planned,
        "feature_snapshot": feature_snapshot,
    }
