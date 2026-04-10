from datetime import datetime
from typing import List

from fastapi import APIRouter, HTTPException, Query

from ..schemas import TelemetryPoint
from ..services.state_store import (
    get_latest_telemetry,
    get_latest_telemetry_by_item,
    get_recent_telemetry_by_item,
    get_telemetry_history_by_item,
)

router = APIRouter(tags=["telemetry"])


@router.get("/telemetry/latest", response_model=List[TelemetryPoint])
def telemetry_latest():
    return get_latest_telemetry()


@router.get("/telemetry/latest/{item_id}", response_model=TelemetryPoint)
def telemetry_latest_by_item(item_id: str):
    point = get_latest_telemetry_by_item(item_id)
    if point is None:
        raise HTTPException(status_code=404, detail=f"Telemetry item '{item_id}' not found")
    return point


@router.get("/telemetry/recent/{item_id}", response_model=List[TelemetryPoint])
def telemetry_recent_by_item(
    item_id: str,
    limit: int = Query(100, ge=1, le=500),
):
    return get_recent_telemetry_by_item(item_id, limit=limit)


@router.get("/telemetry/history/{item_id}", response_model=List[TelemetryPoint])
def telemetry_history_by_item(
    item_id: str,
    from_utc: datetime = Query(..., alias="from"),
    to_utc: datetime = Query(..., alias="to"),
    limit: int = Query(1000, ge=1, le=10000),
):
    if from_utc > to_utc:
        raise HTTPException(status_code=400, detail="'from' must be earlier than or equal to 'to'")

    return get_telemetry_history_by_item(
        item_id,
        from_utc,
        to_utc,
        limit=limit,
    )
