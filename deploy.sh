#!/usr/bin/env bash
set -euo pipefail

cd /opt/iss-anomaly-app

git fetch origin main
git checkout origin/main -- Dockerfile.backend infra/docker-compose.yml
git pull --ff-only origin main
docker compose --env-file .env -f infra/docker-compose.yml --profile app build
docker compose --env-file .env -f infra/docker-compose.yml --profile app up -d
docker ps --format "table {{.Names}}\t{{.Status}}"
curl -sS http://127.0.0.1:8002/health
