from pydantic import BaseModel

class TelemetryPoint(BaseModel):
    item: str
    value: float | None
    timestamp_utc: str
    source: str


class AnomalyEvent(BaseModel):
    detected_at_utc: str
    item: str
    anomaly_type: str
    value_numeric: float | None
    previous_value_numeric: float | None
    threshold_value: float | None
    details: dict
    source: str

class SimulateAnomalyRequest(BaseModel):
    item: str
    mode: str