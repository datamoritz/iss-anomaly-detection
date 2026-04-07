import argparse
import json
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


SCHEMA = pa.schema(
    [
        ("received_at_utc", pa.string()),
        ("received_unix_ms", pa.int64()),
        ("item", pa.string()),
        ("value_raw", pa.string()),
        ("value_numeric", pa.float64()),
        ("source_timestamp_raw", pa.string()),
        ("source", pa.string()),
    ]
)


def parse_args():
    """Parse a small set of options for JSONL to Parquet export."""
    parser = argparse.ArgumentParser(
        description="Export ISS telemetry JSONL files to one Parquet file."
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        help="One or more JSONL files or folders to read.",
    )
    parser.add_argument(
        "--output",
        default="data/parquet/telemetry_export.parquet",
        help="Output Parquet path.",
    )
    return parser.parse_args()


def collect_jsonl_files(inputs):
    """Expand file and folder inputs into a sorted JSONL file list."""
    files = []

    for raw_input in inputs:
        path = Path(raw_input)
        if path.is_dir():
            files.extend(sorted(path.rglob("telemetry_*.jsonl")))
        elif path.is_file():
            files.append(path)

    return sorted(set(files))


def normalize_record(record):
    """Keep the stable collector schema when exporting."""
    received_at_utc = record.get("received_at_utc")
    received_unix_ms = record.get("received_unix_ms")

    if received_unix_ms is None and received_at_utc:
        try:
            received_unix_ms = int(datetime.fromisoformat(received_at_utc).timestamp() * 1000)
        except ValueError:
            received_unix_ms = None

    value_raw = record.get("value_raw")
    if value_raw is None:
        value_raw = record.get("value")

    value_numeric = record.get("value_numeric")
    if value_numeric is None and value_raw is not None:
        try:
            value_numeric = float(value_raw)
        except (TypeError, ValueError):
            value_numeric = None

    source_timestamp_raw = record.get("source_timestamp_raw")
    if source_timestamp_raw is None:
        source_timestamp_raw = record.get("timestamp")

    source = record.get("source")
    if source is None:
        source = "iss_lightstreamer_public"

    return {
        "received_at_utc": received_at_utc,
        "received_unix_ms": received_unix_ms,
        "item": record.get("item"),
        "value_raw": value_raw,
        "value_numeric": value_numeric,
        "source_timestamp_raw": source_timestamp_raw,
        "source": source,
    }


def read_records(files):
    """Read JSONL records from the selected input files."""
    records = []

    for path in files:
        with path.open("r", encoding="utf-8") as infile:
            for line in infile:
                line = line.strip()
                if not line:
                    continue
                records.append(normalize_record(json.loads(line)))

    return records


def main():
    args = parse_args()
    files = collect_jsonl_files(args.inputs)

    if not files:
        print("No telemetry JSONL files found.")
        return

    records = read_records(files)
    if not records:
        print("No telemetry records found.")
        return

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pylist(records, schema=SCHEMA)
    pq.write_table(table, output_path)

    print(f"Files read: {len(files)}")
    print(f"Rows written: {table.num_rows}")
    print(f"Parquet file: {output_path.resolve()}")


if __name__ == "__main__":
    main()
