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
    trigger_source: str | None = None
    is_simulated: bool = False


class SimulateAnomalyRequest(BaseModel):
    item: str
    mode: str


class SubscriptionCreateRequest(BaseModel):
    email: str
    item_id: str | None = None
    anomaly_type: str | None = None


class SubscriptionVerifyRequest(BaseModel):
    token: str


class SubscriptionDeleteRequest(BaseModel):
    token: str


class SubscriptionResponse(BaseModel):
    ok: bool
    message: str
    subscription_id: int | None = None


class ItemMetadata(BaseModel):
    item: str
    label: str
    unit: str
    category: str
    description: str
