import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def parse_args():
    """Parse a small set of options for folder summarization."""
    parser = argparse.ArgumentParser(
        description="Summarize hourly ISS telemetry JSONL files."
    )
    parser.add_argument(
        "folder",
        nargs="?",
        default="data/raw",
        help="Folder to scan for telemetry_*.jsonl files.",
    )
    parser.add_argument(
        "--ignore-current-hour",
        action="store_true",
        help="Skip the file for the current UTC hour based on filename.",
    )
    return parser.parse_args()


def current_hour_filename():
    """Return the current UTC hour filename for optional skipping."""
    now_utc = datetime.now(timezone.utc)
    return f"telemetry_{now_utc.strftime('%Y-%m-%d_%H')}.jsonl"


def collect_files(folder, ignore_current_hour):
    """Find hourly JSONL files under the chosen folder."""
    base_path = Path(folder)
    files = sorted(base_path.rglob("telemetry_*.jsonl"))

    if ignore_current_hour:
        active_name = current_hour_filename()
        files = [path for path in files if path.name != active_name]

    return files


def summarize_files(files):
    """Read the files once and build simple per-item counts."""
    item_counts = defaultdict(int)
    first_timestamp = None
    last_timestamp = None
    total_bytes = 0
    total_messages = 0

    for path in files:
        total_bytes += path.stat().st_size

        with path.open("r", encoding="utf-8") as infile:
            for line in infile:
                line = line.strip()
                if not line:
                    continue

                record = json.loads(line)
                item = record.get("item", "")
                received_at = record.get("received_at_utc")

                item_counts[item] += 1
                total_messages += 1

                if received_at:
                    timestamp = datetime.fromisoformat(received_at)
                    if first_timestamp is None or timestamp < first_timestamp:
                        first_timestamp = timestamp
                    if last_timestamp is None or timestamp > last_timestamp:
                        last_timestamp = timestamp

    return item_counts, first_timestamp, last_timestamp, total_bytes, total_messages


def main():
    args = parse_args()
    files = collect_files(args.folder, args.ignore_current_hour)

    if not files:
        print("No telemetry files found.")
        return

    item_counts, first_timestamp, last_timestamp, total_bytes, total_messages = summarize_files(files)

    runtime_seconds = 0.0
    if first_timestamp is not None and last_timestamp is not None:
        runtime_seconds = max((last_timestamp - first_timestamp).total_seconds(), 0.0)

    print(f"Folder: {Path(args.folder).resolve()}")
    print(f"Files read: {len(files)}")
    print(f"Total messages: {total_messages}")
    print(f"Total file size bytes: {total_bytes}")
    print(f"Runtime seconds observed: {runtime_seconds:.1f}")
    print()
    print(f"{'item':<14} {'messages':>10} {'updates/sec':>12}")
    print("-" * 40)

    for item in sorted(item_counts):
        updates_per_second = 0.0
        if runtime_seconds > 0:
            updates_per_second = item_counts[item] / runtime_seconds

        print(
            f"{item:<14} "
            f"{item_counts[item]:>10} "
            f"{updates_per_second:>12.6f}"
        )


if __name__ == "__main__":
    main()
