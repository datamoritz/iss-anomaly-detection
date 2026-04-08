from fastapi import APIRouter, HTTPException

from app.schemas import SimulateAnomalyRequest
from app.services.simulator import publish_simulated_anomaly

router = APIRouter(tags=["simulation"])


@router.post("/simulate-anomaly")
def simulate_anomaly(request: SimulateAnomalyRequest):
    try:
        event = publish_simulated_anomaly(request)
        return {
            "ok": True,
            "message": "Simulated telemetry event published to Kafka",
            "event": event,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Simulation failed: {exc}")