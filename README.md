# ISS Telemetry Pipeline

Collects public ISS telemetry, stores raw hourly JSONL files, processes live events through Kafka, detects anomalies, and exposes live state through a FastAPI backend.

## Current Shape

- `collector/`
  - raw source-of-truth collection
  - hourly JSONL rotation
  - folder summary and Parquet export helpers
- `pipeline/`
  - Kafka ingest from Lightstreamer
  - worker for latest-state caching, bounded recent history, durable telemetry history, and anomaly detection
- `api/`
  - FastAPI backend for items, latest telemetry, recent anomalies, and simulation
- `frontend/`
  - standalone Vite/React dashboard app for live telemetry and anomaly monitoring
- `config/`
  - shared item metadata and anomaly rules
- `analysis/`
  - notebook-based offline inspection

## Architecture

```text
Lightstreamer (ISS telemetry)
        ↓
pipeline/ingest_to_kafka.py
        ↓
Kafka (telemetry.raw)
        ↓
pipeline/worker.py
   ├─ Redis (latest state)
   ├─ Redis (recent_history:<item>, bounded)
   ├─ PostgreSQL (telemetry history + anomaly history)
   └─ Kafka (anomaly.events)
        ↓
notifications/service.py
        ↓
     Email alerts
        ↓
FastAPI backend
        ↓
Frontend
```

Current frontend MVP:

- metadata-driven parameter selector
- live sampled telemetry chart
- in-session history placeholder controls
- anomaly markers on the chart
- recent anomalies table
- simulation controls

## Raw Collector

Main collector:

- `collector/main.py`

Reference script kept for documentation:

- `collector/collect_1h_experiment.py`

Raw storage format:

```text
data/raw/YYYY-MM-DD/telemetry_YYYY-MM-DD_HH.jsonl
```

Gap diagnostics:

```text
data/diagnostics/collector_gaps.jsonl
data/diagnostics/ingest_gaps.jsonl
```

Manifest:

```text
data/manifest.json
```

Stable raw schema:

- `received_at_utc`
- `received_unix_ms`
- `item`
- `value_raw`
- `value_numeric`
- `source_timestamp_raw`
- `source`

Collector item set:

- `S0000004`
- `NODE3000012`
- `P1000003`
- `NODE3000013`
- `S1000003`
- `USLAB000059`
- `NODE3000005`
- `NODE3000009`

## API

Main app:

- `api/app/main.py`

Current endpoints:

- `GET /`
- `GET /health`
- `GET /api/v1/items`
- `GET /api/v1/items/{item_id}`
- `GET /api/v1/telemetry/latest`
- `GET /api/v1/telemetry/latest/{item_id}`
- `GET /api/v1/telemetry/recent/{item_id}`
- `GET /api/v1/telemetry/history/{item_id}`
- `POST /api/v1/injections`
- `GET /api/v1/anomalies/recent`
- `GET /api/v1/anomalies/recent/{item_id}`
- `POST /api/v1/simulate-anomaly`
- `POST /api/v1/subscriptions`
- `POST /api/v1/subscriptions/verify`
- `GET /api/v1/subscriptions/verify`
- `POST /api/v1/subscriptions/unsubscribe`
- `GET /api/v1/subscriptions/unsubscribe`
- `DELETE /api/v1/subscriptions/{subscription_id}`

Notes:

- API config now comes from app settings / environment variables.
- CORS middleware is enabled from settings.
- Latest telemetry is read from Redis hash `latest_state`.
- Redis also stores bounded recent history lists under `recent_history:<item_id>`.
- Redis also stores bounded normal-only history lists under `normal_history:<item_id>`
  and rolling feature stats in hash `feature_state`.
- Postgres stores durable telemetry history for this app in `telemetry_history`.
- Postgres also stores email subscriptions and notification logs.
- Verification emails are sent by the API; anomaly alert emails are sent by the separate notification consumer.
- `/health` now checks Redis, Postgres, and Kafka.
- `/health/live` is a simple liveness route.
- Postgres schema setup is deterministic and runs from shared code on API startup and worker startup.
- Telemetry history retention is enforced once per day in the worker and deletes rows older than 28 days by default.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create a local env file when needed:

```bash
cp .env.example .env
```

## Run Commands

Raw collector:

```bash
python -m collector.main
python -m collector.main --counts-interval 60
```

Summarize raw files:

```bash
python -m collector.summarize_folder data/raw --ignore-current-hour
```

Export to Parquet:

```bash
python -m collector.export_to_parquet data/raw --output data/parquet/telemetry_all.parquet
```

Kafka ingest:

```bash
python -m pipeline.ingest_to_kafka
```

The raw collector and app ingest now both include:

- connection lifecycle logging
- heartbeat warnings after prolonged silence
- forced reconnect after longer silence
- persisted gap diagnostics when message gaps exceed the configured threshold

Worker:

```bash
python -m pipeline.worker
```

API:

```bash
uvicorn api.app.main:app --reload --host 0.0.0.0 --port 8000
```

Notification consumer:

```bash
python -m notifications.service
```

Frontend:

```bash
cd frontend
npm install
npm run dev -- --port 5173
```

Default frontend URL:

```text
http://localhost:5173
```

Default backend URL expected by the frontend:

```text
http://localhost:8000
```

## Docker Compose

Infrastructure only:

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d
```

Full backend stack in containers:

```bash
docker compose --env-file .env -f infra/docker-compose.yml --profile app up -d --build
```

Container services:

- `postgres`
- `redis`
- `kafka`
- `api`
- `worker`
- `ingest`
- `notifications`
- `injections`

Important container notes:

- The backend image is built from [Dockerfile.backend](/Users/moritzknodler/Documents/00_Lectures/0_Spring%202026/Datacenters/Project/Code/Dockerfile.backend).
- App containers mount [data](/Users/moritzknodler/Documents/00_Lectures/0_Spring%202026/Datacenters/Project/Code/data) at `/app/data`.
- Kafka uses `localhost:9092` for host access and `kafka:19092` for container-to-container access.
- `KAFKA_INJECTION_TOPIC` defaults to `injection.jobs`.
- `KAFKA_INJECTION_CONSUMER_GROUP` defaults to `iss-injection-worker`.
- `REDIS_TELEMETRY_LIVE_CHANNEL` defaults to `telemetry:live`.
- `PROTOTYPE_LIBRARY_DIR` defaults to `data/anomaly_prototypes/smap_final9_v01`.
- `INJECTION_MAX_POINTS` defaults to `500`.
- Set `CORS_ALLOW_ORIGINS` in `.env` to include your future Vercel frontend domain.
- `COLLECTOR_DIAGNOSTICS_PATH` defaults to `data/diagnostics/collector_gaps.jsonl`.
- `APP_INGEST_DIAGNOSTICS_PATH` defaults to `data/diagnostics/ingest_gaps.jsonl`.
- `COLLECTOR_HEARTBEAT_WARN_SECONDS` and `APP_INGEST_HEARTBEAT_WARN_SECONDS` default to `15`.
- `COLLECTOR_HEARTBEAT_RECONNECT_SECONDS` and `APP_INGEST_HEARTBEAT_RECONNECT_SECONDS` default to `45`.
- `COLLECTOR_GAP_THRESHOLD_SECONDS` and `APP_INGEST_GAP_THRESHOLD_SECONDS` default to `60`.
- `REDIS_RECENT_HISTORY_LIMIT` defaults to `100`.
- `REDIS_FEATURE_WINDOW_SIZE` defaults to `100`.
- `TELEMETRY_RETENTION_DAYS` defaults to `28`.
- `WORKER_REDIS_RETRY_MAX_SECONDS` defaults to `60`.
- `WORKER_REDIS_HEALTH_DEGRADED_AFTER_SECONDS` defaults to `300`.
- `DEFAULT_NOTIFICATION_COOLDOWN_MINUTES` defaults to `10`.
- Email alert env vars:
  - `EMAIL_PROVIDER`
  - `RESEND_API_KEY`
  - `EMAIL_FROM`
  - `APP_BASE_URL`

## Important Notes

- JSONL remains the raw source of truth.
- Parquet export is offline only.
- The raw collector is separate from the Kafka/Redis/Postgres/API path.
- Your existing hourly JSONL collection for neural-net work can remain separate from the app pipeline.
- The frontend now preloads recent telemetry from Redis and can request historical telemetry from Postgres.
- The frontend now bootstraps history over HTTP and receives live telemetry updates over FastAPI WebSockets at `/ws/telemetry`.
- For longer-running server use, prefer `tmux`, `systemd`, or later K8s pods.
