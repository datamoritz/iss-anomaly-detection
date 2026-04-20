from pydantic import BaseModel


class TelemetryPoint(BaseModel):
    item: str
    value: float | None
    timestamp_utc: str
    source: str


class ContinuousAnglePoint(BaseModel):
    parameter: str
    item: str
    angle_deg: float | None
    angle_rad: float | None
    angle_sin: float | None
    angle_cos: float | None
    timestamp_utc: str
    source: str


class FeatureState(BaseModel):
    item: str
    window_size: int
    value_count: int
    baseline_mean: float | None
    baseline_std: float | None
    median_delta_t_seconds: float | None
    updated_at_utc: str
    source: str


class InjectionJobCreateRequest(BaseModel):
    prototype_id: str
    item_id: str
    severity: float = 1.0
    time_scale: float = 1.0
    recenter: bool = True


class InjectionJobResponse(BaseModel):
    ok: bool
    message: str
    job_id: str
    status: str
    prototype_id: str
    item_id: str
    severity: float
    time_scale: float
    recenter: bool
    points_planned: int
    feature_snapshot: dict | None = None


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
