import asyncio

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from redis import asyncio as redis_async

from ..config import settings


router = APIRouter(tags=["stream"])


def create_async_redis_client():
    return redis_async.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        password=settings.REDIS_PASSWORD or None,
        decode_responses=True,
    )


@router.websocket("/ws/telemetry")
async def telemetry_ws(websocket: WebSocket):
    await websocket.accept()

    redis_client = create_async_redis_client()
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(settings.REDIS_TELEMETRY_LIVE_CHANNEL)

    try:
        while True:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=1.0,
            )
            if message is None:
                await asyncio.sleep(0.1)
                continue

            payload = message.get("data")
            if payload is None:
                continue

            await websocket.send_text(payload)
    except WebSocketDisconnect:
        pass
    finally:
        try:
            await pubsub.unsubscribe(settings.REDIS_TELEMETRY_LIVE_CHANNEL)
        except Exception:
            pass
        try:
            await pubsub.close()
        except Exception:
            pass
        try:
            await redis_client.close()
        except Exception:
            pass
