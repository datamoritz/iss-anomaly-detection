

from fastapi import FastAPI
from app.routers import health, telemetry, anomalies

app = FastAPI(
    title="ISS Telemetry API",
    version="0.1.0",
)

@app.get("/")
def root():
    return {"message": "ISS Telemetry API is running"}

app.include_router(health.router)
app.include_router(telemetry.router, prefix="/api/v1")
app.include_router(anomalies.router, prefix="/api/v1")
