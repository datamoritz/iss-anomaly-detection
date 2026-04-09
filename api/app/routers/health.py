from fastapi import APIRouter
from fastapi.responses import JSONResponse

from config.runtime import check_kafka, check_postgres, check_redis

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
    healthy = all(check["ok"] for check in checks.values())
    payload = {
        "status": "ok" if healthy else "degraded",
        "checks": checks,
    }
    status_code = 200 if healthy else 503
    return JSONResponse(status_code=status_code, content=payload)
