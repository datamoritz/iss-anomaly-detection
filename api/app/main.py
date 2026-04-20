from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config.runtime import create_postgres_connection, ensure_postgres_schema

from .config import settings
from .routers import health, telemetry, anomalies, simulation, items, subscriptions, injections


@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = None
    try:
        conn = create_postgres_connection()
        ensure_postgres_schema(conn)
        yield
    finally:
        if conn is not None:
            conn.close()

app = FastAPI(
    title=settings.APP_NAME,
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {
        "message": f"{settings.APP_NAME} is running",
        "env": settings.APP_ENV,
    }


app.include_router(health.router)
app.include_router(items.router, prefix="/api/v1")
app.include_router(telemetry.router, prefix="/api/v1")
app.include_router(anomalies.router, prefix="/api/v1")
app.include_router(simulation.router, prefix="/api/v1")
app.include_router(injections.router, prefix="/api/v1")
app.include_router(subscriptions.router, prefix="/api/v1")
