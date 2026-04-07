import csv
import json
import signal
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from lightstreamer.client import (
    ClientListener,
    LightstreamerClient,
    Subscription,
    SubscriptionListener,
)


# Run the experiment for 1 hour by default.
DEFAULT_RUNTIME_SECONDS = 60 * 60
STATS_INTERVAL_SECONDS = 10

# Use the exact telemetry items requested for the 1-hour experiment.
TELEMETRY_ITEMS = [
    "USLAB000058",   # Cabin Pressure
    "USLAB000059",   # Cabin Temperature
    "USLAB000055",   # Lab ppCO2
    "NODE3000012",   # Avionics Cooling Fluid Temp (Node 3)
    "NODE3000013",   # Air Cooling Fluid Temp (Node 3)
    "S1000003",      # Loop A PM Out Temp
    "P1000003",      # Loop B PM Out Temp
    "S0000001",      # Starboard radiator joint position
    "S0000002",      # Port radiator joint position
    "S0000004",      # Port SARJ angle position
    "Z1000009",      # CMG 1 wheel speed
    "Z1000005",      # CMG 1 spin motor current
    "NODE3000005",   # Urine Tank [%]
]

TELEMETRY_FIELDS = ["Value", "TimeStamp"]


def utc_now_iso():
    """Return the current UTC time in ISO format for logging."""
    return datetime.now(timezone.utc).isoformat()


def make_output_paths():
    """Create output folders and build timestamped filenames."""
    run_stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    raw_dir = Path("data/raw")
    summary_dir = Path("data/summary")
    raw_dir.mkdir(parents=True, exist_ok=True)
    summary_dir.mkdir(parents=True, exist_ok=True)

    raw_path = raw_dir / f"telemetry_{run_stamp}.jsonl"
    summary_path = summary_dir / f"telemetry_summary_{run_stamp}.csv"
    return raw_path, summary_path


def make_empty_stats():
    """Create the per-item stats structure used during the run."""
    stats = {}
    for item in TELEMETRY_ITEMS:
        stats[item] = {
            "total_messages": 0,
            "repeated_messages": 0,
            "latest_value": "",
            "min_value": None,
            "max_value": None,
            "last_update_time": None,
            "delta_sum_seconds": 0.0,
        }
    return stats


RAW_LOG_PATH, SUMMARY_PATH = make_output_paths()
STOP_EVENT = threading.Event()
STATS_LOCK = threading.Lock()
START_MONOTONIC = time.monotonic()
ITEM_STATS = make_empty_stats()


class StatusPrinter(ClientListener):
    """Print connection status so the user can see what the client is doing."""

    def onStatusChange(self, status):
        print(f"[client] status: {status}")

    def onServerError(self, error_code, error_message):
        print(f"[client] server error {error_code}: {error_message}")


class TelemetryRecorder(SubscriptionListener):
    """Store each raw update and update the in-memory per-item stats."""

    def onSubscription(self):
        print("[subscription] subscribed successfully")

    def onSubscriptionError(self, code, message):
        print(f"[subscription] error {code}: {message}")
        STOP_EVENT.set()

    def onItemUpdate(self, update):
        item = update.getItemName()
        value = update.getValue("Value")
        source_timestamp = update.getValue("TimeStamp")
        received_at_utc = utc_now_iso()
        received_monotonic = time.monotonic()

        # Save the raw event exactly as it arrives, plus local receive time.
        record = {
            "received_at_utc": received_at_utc,
            "item": item,
            "value": value,
            "timestamp": source_timestamp,
        }

        with RAW_LOG_PATH.open("a", encoding="utf-8") as log_file:
            log_file.write(json.dumps(record, ensure_ascii=True) + "\n")

        with STATS_LOCK:
            item_stats = ITEM_STATS[item]

            if item_stats["total_messages"] > 0 and value == item_stats["latest_value"]:
                item_stats["repeated_messages"] += 1

            last_update_time = item_stats["last_update_time"]
            if last_update_time is not None:
                item_stats["delta_sum_seconds"] += received_monotonic - last_update_time

            item_stats["total_messages"] += 1
            item_stats["latest_value"] = value if value is not None else ""
            item_stats["last_update_time"] = received_monotonic

            # Only update min/max when the value is numeric.
            try:
                numeric_value = float(value)
            except (TypeError, ValueError):
                numeric_value = None

            if numeric_value is not None:
                current_min = item_stats["min_value"]
                current_max = item_stats["max_value"]

                if current_min is None or numeric_value < current_min:
                    item_stats["min_value"] = numeric_value
                if current_max is None or numeric_value > current_max:
                    item_stats["max_value"] = numeric_value


def repeated_percent(item_stats):
    """Return the percent of updates that repeated the previous value."""
    total_messages = item_stats["total_messages"]
    if total_messages == 0:
        return ""
    return f"{(item_stats['repeated_messages'] / total_messages) * 100:.2f}"


def updates_per_second(item_stats, runtime_seconds):
    """Return the observed per-item message rate."""
    if runtime_seconds <= 0:
        return 0.0
    return item_stats["total_messages"] / runtime_seconds


def average_delta_seconds(item_stats):
    """Return the average time between updates for one item."""
    total_messages = item_stats["total_messages"]
    if total_messages < 2:
        return ""
    return item_stats["delta_sum_seconds"] / (total_messages - 1)


def format_number(value):
    """Format numeric values cleanly and leave blanks unchanged."""
    if value in ("", None):
        return ""
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def print_stats_table():
    """Print rolling per-item stats every 10 seconds."""
    runtime_seconds = time.monotonic() - START_MONOTONIC

    with STATS_LOCK:
        snapshot = {
            item: dict(item_stats)
            for item, item_stats in ITEM_STATS.items()
        }

    print()
    print(f"[stats] {utc_now_iso()} runtime={runtime_seconds:.1f}s")
    header = (
        f"{'item':<14} {'messages':>8} {'upd/sec':>8} {'repeat%':>8} "
        f"{'latest_value':>22} {'min':>12} {'max':>12}"
    )
    print(header)
    print("-" * len(header))

    for item in TELEMETRY_ITEMS:
        item_stats = snapshot[item]
        latest_value = str(item_stats["latest_value"])
        if len(latest_value) > 22:
            latest_value = latest_value[:19] + "..."

        print(
            f"{item:<14} "
            f"{item_stats['total_messages']:>8} "
            f"{updates_per_second(item_stats, runtime_seconds):>8.3f} "
            f"{repeated_percent(item_stats):>8} "
            f"{latest_value:>22} "
            f"{format_number(item_stats['min_value']):>12} "
            f"{format_number(item_stats['max_value']):>12}"
        )


def write_summary_csv(total_runtime_seconds):
    """Save one CSV summary when the experiment ends."""
    with STATS_LOCK:
        snapshot = {
            item: dict(item_stats)
            for item, item_stats in ITEM_STATS.items()
        }

    with SUMMARY_PATH.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            [
                "item",
                "total_messages",
                "observed_updates_per_sec",
                "repeated_value_percent",
                "average_time_delta_between_updates",
                "min",
                "max",
                "total_runtime_seconds",
            ]
        )

        for item in TELEMETRY_ITEMS:
            item_stats = snapshot[item]
            writer.writerow(
                [
                    item,
                    item_stats["total_messages"],
                    f"{updates_per_second(item_stats, total_runtime_seconds):.6f}",
                    repeated_percent(item_stats),
                    format_number(average_delta_seconds(item_stats)),
                    format_number(item_stats["min_value"]),
                    format_number(item_stats["max_value"]),
                    f"{total_runtime_seconds:.3f}",
                ]
            )

    print(f"[summary] wrote {SUMMARY_PATH.resolve()}")


def stats_loop():
    """Print stats every 10 seconds until the run ends."""
    while not STOP_EVENT.wait(STATS_INTERVAL_SECONDS):
        print_stats_table()


def timer_loop(runtime_seconds):
    """Stop the script automatically after the default runtime."""
    if not STOP_EVENT.wait(runtime_seconds):
        print(f"\n[client] reached {runtime_seconds} seconds, stopping...")
        STOP_EVENT.set()


def handle_shutdown(signum, frame):
    """Stop cleanly when the user presses Ctrl+C."""
    print("\n[client] shutting down...")
    STOP_EVENT.set()


def main():
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    print("[client] connecting to the ISS public Lightstreamer feed...")
    print(f"[client] raw log file: {RAW_LOG_PATH.resolve()}")
    print(f"[client] summary file: {SUMMARY_PATH.resolve()}")
    print(f"[client] runtime: {DEFAULT_RUNTIME_SECONDS} seconds")

    client = LightstreamerClient("https://push.lightstreamer.com", "ISSLIVE")
    client.addListener(StatusPrinter())

    subscription = Subscription("MERGE", TELEMETRY_ITEMS, TELEMETRY_FIELDS)
    subscription.setRequestedSnapshot("yes")
    subscription.addListener(TelemetryRecorder())

    client.subscribe(subscription)
    client.connect()

    threading.Thread(target=stats_loop, daemon=True).start()
    threading.Thread(
        target=timer_loop,
        args=(DEFAULT_RUNTIME_SECONDS,),
        daemon=True,
    ).start()

    STOP_EVENT.wait()
    client.disconnect()

    total_runtime_seconds = time.monotonic() - START_MONOTONIC
    print_stats_table()
    write_summary_csv(total_runtime_seconds)
    print("[client] disconnected")


if __name__ == "__main__":
    main()
