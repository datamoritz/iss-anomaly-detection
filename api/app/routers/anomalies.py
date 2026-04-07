from typing import List

from fastapi import APIRouter, Query

from app.schemas import AnomalyEvent
from app.services.state_store import get_recent_anomalies, get_recent_anomalies_by_item

router = APIRouter(tags=["anomalies"])


@router.get("/anomalies/recent", response_model=List[AnomalyEvent])
def anomalies_recent(limit: int = Query(50, ge=1, le=1000)):
    return get_recent_anomalies()[:limit]


@router.get("/anomalies/recent/{item_id}", response_model=List[AnomalyEvent])
def anomalies_recent_by_item(item_id: str, limit: int = Query(50, ge=1, le=1000)):
    events = get_recent_anomalies_by_item(item_id)
    return events[:limit]





