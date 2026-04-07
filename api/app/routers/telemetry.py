from typing import List

from fastapi import APIRouter, HTTPException

from app.schemas import TelemetryPoint
from app.services.state_store import get_latest_telemetry, get_latest_telemetry_by_item

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