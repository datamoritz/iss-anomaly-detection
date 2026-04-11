from datetime import datetime, timezone

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from config.runtime import (
    check_kafka,
    check_postgres,
    check_redis,
    create_postgres_connection,
    get_service_status,
)
from config.settings import settings

router = APIRouter(tags=["health"])


@router.get("/health/live")
def health_live():
    return {"status": "ok"}


@router.get("/health")
def health_check():
    redis_ok, redis_message = check_redis()
    postgres_ok, postgres_message = check_postgres()
    kafka_ok, kafka_message = check_kafka()

    checks = {
        "redis": {"ok": redis_ok, "message": redis_message},
        "postgres": {"ok": postgres_ok, "message": postgres_message},
        "kafka": {"ok": kafka_ok, "message": kafka_message},
    }

    worker_cache_ok = True
    worker_cache_message = "ok"
    if postgres_ok:
        conn = None
        try:
            conn = create_postgres_connection()
            row = get_service_status(conn, "worker_redis_cache")
            if row is not None:
                _, status, message, degraded_since, _ = row
                if status == "degraded" and degraded_since is not None:
                    duration_seconds = (
                        datetime.now(timezone.utc) - degraded_since
                    ).total_seconds()
                    if duration_seconds >= settings.WORKER_REDIS_HEALTH_DEGRADED_AFTER_SECONDS:
                        worker_cache_ok = False
                worker_cache_message = message or status
        except Exception as exc:
            worker_cache_ok = False
            worker_cache_message = str(exc)
        finally:
            if conn is not None:
                conn.close()

    checks["worker_redis_cache"] = {
        "ok": worker_cache_ok,
        "message": worker_cache_message,
    }

    healthy = all(check["ok"] for check in checks.values())
    payload = {
        "status": "ok" if healthy else "degraded",
        "checks": checks,
    }
    status_code = 200 if healthy else 503
    return JSONResponse(status_code=status_code, content=payload)
