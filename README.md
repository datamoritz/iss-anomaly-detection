# ISS Telemetry Collector

This project currently focuses on simple ISS public telemetry collection and offline analysis.

## Current State

- `main.py`
  - Long-running collector for Linux/server use
  - Connects to the public ISS Lightstreamer feed
  - Subscribes to 8 selected telemetry items
  - Writes hourly rotating raw JSONL files
- `collect_1h_experiment.py`
  - Earlier 1-hour experiment script kept for reference
- `summarize_folder.py`
  - Reads saved hourly JSONL files and prints simple per-item counts
- `export_to_parquet.py`
  - Converts one or more raw JSONL files into one Parquet file
- `analyze_telemetry.ipynb`
  - Simple notebook for inspecting saved telemetry files

## Raw Storage

Raw files are the source of truth.

Path format:

```text
data/raw/YYYY-MM-DD/telemetry_YYYY-MM-DD_HH.jsonl
```

Example:

```text
data/raw/2026-04-06/telemetry_2026-04-06_23.jsonl
```

## Raw JSONL Schema

Each row uses this stable schema:

- `received_at_utc`
- `received_unix_ms`
- `item`
- `value_raw`
- `value_numeric`
- `source_timestamp_raw`
- `source`

The collector also writes:

- `data/manifest.json`

## Selected Items In `main.py`

- `S0000004`
- `NODE3000012`
- `P1000003`
- `NODE3000013`
- `S1000003`
- `USLAB000059`
- `NODE3000005`
- `NODE3000009`

## Setup

```bash
cd "/Users/moritzknodler/Documents/00_Lectures/0_Spring 2026/Datacenters/Project/Code"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run Collector

```bash
source .venv/bin/activate
python main.py
```

Optional count interval:

```bash
python main.py --counts-interval 60
```

## Summarize Saved Files

```bash
source .venv/bin/activate
python summarize_folder.py data/raw --ignore-current-hour
```

## Export To Parquet

```bash
source .venv/bin/activate
python export_to_parquet.py data/raw --output data/parquet/telemetry_all.parquet
```

## Notes

- JSONL remains the raw source of truth.
- Parquet export is offline only and does not affect collection.
- No Kafka, Redis, Postgres, FastAPI, or Docker are part of the current system.
- For a 2-week server run, use `tmux` or `systemd`.




--------------

Lightstreamer (ISS public feed via WebSocket)
        ↓
ingest_to_kafka.py (producer)
        ↓
Kafka (topic: telemetry.raw)
        ↓
worker.py (consumer + processing)
   ├─ Redis (latest state cache)
   ├─ PostgreSQL (anomaly history)
   └─ Kafka (topic: anomaly.events)


   # ISS Telemetry Pipeline

This project collects real ISS public telemetry, stores it as raw files, and processes it through a simple real-time streaming pipeline for anomaly detection.

The system started as an offline telemetry collection setup and was extended into a small datacenter-style MVP using Kafka, Redis, PostgreSQL, and Docker.

---

## Current Scope

This repository currently includes two parallel but connected parts:

1. **Raw telemetry collection and offline analysis**
2. **Real-time streaming pipeline for anomaly detection**

---

# 1. Raw Telemetry Collection

## Purpose

The raw collector is used to continuously gather ISS telemetry over time for:
- exploratory analysis
- feature understanding
- future anomaly detection model development
- historical inspection and plotting

Raw files are treated as the **source of truth**.

---

## Main Collector

### `main.py`
Long-running collector for Linux/server use.

It:
- connects to the public ISS Lightstreamer feed
- subscribes to 8 selected telemetry items
- writes hourly rotating raw JSONL files
- writes a `manifest.json` describing the run

### `collect_1h_experiment.py`
Earlier 1-hour experiment script kept for reference.

### `summarize_folder.py`
Reads saved hourly JSONL files and prints simple per-item counts.

### `export_to_parquet.py`
Converts one or more raw JSONL files into a Parquet file for offline analysis.

### `analyze_telemetry.ipynb`
Notebook for inspecting saved telemetry files.

---

## Raw Storage

Raw files are stored hourly using this format:

```text
data/raw/YYYY-MM-DD/telemetry_YYYY-MM-DD_HH.jsonl