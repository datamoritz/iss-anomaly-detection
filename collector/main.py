import argparse
import json
import signal
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from config.settings import settings
from config.stream_monitoring import StreamMonitor, utc_now_iso
from lightstreamer.client import (
    ClientListener,
    LightstreamerClient,
    Subscription,
    SubscriptionListener,
)


DEFAULT_COUNTS_INTERVAL_SECONDS = 60
SOURCE_NAME = "iss_lightstreamer_public"
SCHEMA_FIELDS = [
    "received_at_utc",
    "received_unix_ms",
    "item",
    "value_raw",
    "value_numeric",
    "source_timestamp_raw",
    "source",
]

TELEMETRY_ITEMS = [
    "S0000004",      # Port SARJ angle position
    "NODE3000012",   # Avionics Cooling Fluid Temp (Node 3)
    "P1000003",      # Loop B PM Out Temp
    "NODE3000013",   # Air Cooling Fluid Temp (Node 3)
    "S1000003",      # Loop A PM Out Temp
    "USLAB000059",   # Cabin Temperature
    "NODE3000005",   # Urine Tank [%]
    "NODE3000009",   # Clean Water Tank
]

TELEMETRY_FIELDS = ["Value", "TimeStamp"]
STOP_EVENT = threading.Event()
STATS_LOCK = threading.Lock()
START_MONOTONIC = time.monotonic()
RAW_OUTPUT_ROOT = Path(settings.COLLECTOR_RAW_ROOT)
MANIFEST_PATH = Path(settings.COLLECTOR_MANIFEST_PATH)
def parse_numeric_value(value_raw):
    """Parse a float when possible and otherwise return None."""
    try:
        return float(value_raw)
    except (TypeError, ValueError):
        return None


def path_for_hour(received_at):
    """Build the output path from the event receive time."""
    day_folder = RAW_OUTPUT_ROOT / received_at.strftime("%Y-%m-%d")
    day_folder.mkdir(parents=True, exist_ok=True)
    filename = f"telemetry_{received_at.strftime('%Y-%m-%d_%H')}.jsonl"
    return day_folder / filename


def write_manifest():
    """Write a tiny manifest that documents this collection setup."""
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)

    manifest = {
        "started_at_utc": utc_now_iso(),
        "selected_items": TELEMETRY_ITEMS,
        "schema_fields": SCHEMA_FIELDS,
        "output_root": settings.COLLECTOR_RAW_ROOT,
        "version": "1.0",
    }

    with MANIFEST_PATH.open("w", encoding="utf-8") as manifest_file:
        json.dump(manifest, manifest_file, indent=2)
        manifest_file.write("\n")

    print(f"[manifest] wrote {MANIFEST_PATH.resolve()}")


def make_empty_stats():
    """Create a simple per-item counter structure."""
    stats = {}
    for item in TELEMETRY_ITEMS:
        stats[item] = {
            "total_messages": 0,
            "latest_value": "",
        }
    return stats


ITEM_STATS = make_empty_stats()
MONITOR = StreamMonitor(
    stream_name="collector",
    selected_items=TELEMETRY_ITEMS,
    diagnostics_path=settings.COLLECTOR_DIAGNOSTICS_PATH,
    gap_threshold_seconds=settings.COLLECTOR_GAP_THRESHOLD_SECONDS,
    heartbeat_warn_seconds=settings.COLLECTOR_HEARTBEAT_WARN_SECONDS,
    heartbeat_reconnect_seconds=settings.COLLECTOR_HEARTBEAT_RECONNECT_SECONDS,
)


class HourlyJsonlWriter:
    """Keeps the current hourly JSONL file open and rotates safely."""

    def __init__(self):
        self.current_hour_key = None
        self.current_path = None
        self.current_file = None
        self.lock = threading.Lock()

    def write_record(self, record, received_at):
        """Rotate if needed, then write the current event to the right file."""
        target_hour_key = received_at.strftime("%Y-%m-%d_%H")
        target_path = path_for_hour(received_at)

        with self.lock:
            if self.current_hour_key != target_hour_key:
                self._rotate_to(target_hour_key, target_path)

            self.current_file.write(json.dumps(record, ensure_ascii=True) + "\n")
            self.current_file.flush()

    def _rotate_to(self, hour_key, path):
        """Close the old file and open the new hour file."""
        if self.current_file is not None:
            self.current_file.flush()
            self.current_file.close()
            print(f"[writer] rotated file: {self.current_path.resolve()}")

        self.current_hour_key = hour_key
        self.current_path = path
        self.current_file = path.open("a", encoding="utf-8")
        print(f"[writer] current output file: {path.resolve()}")

    def close(self):
        """Flush and close the current file on shutdown."""
        with self.lock:
            if self.current_file is not None:
                self.current_file.flush()
                self.current_file.close()
                print(f"[writer] closed file: {self.current_path.resolve()}")
                self.current_file = None


WRITER = HourlyJsonlWriter()


class StatusPrinter(ClientListener):
    """Print connection status so the user can monitor the recorder."""

    def onStatusChange(self, status):
        print(f"[client] status: {status}")
        lowered = status.lower()
        if "disconnected" in lowered:
            MONITOR.mark_disconnect(reason=status)
        elif "connected" in lowered:
            MONITOR.mark_connected(status)

    def onServerError(self, error_code, error_message):
        print(f"[client] server error {error_code}: {error_message}")
        MONITOR.log_event(
            "server_error",
            error_code=error_code,
            error_message=error_message,
        )


class TelemetryRecorder(SubscriptionListener):
    """Write each event as JSONL and keep small in-memory counters."""

    def onSubscription(self):
        print("[subscription] subscribed successfully")
        MONITOR.mark_subscribed()

    def onSubscriptionError(self, code, message):
        print(f"[subscription] error {code}: {message}")
        MONITOR.log_event("subscription_error", code=code, message=message)
        STOP_EVENT.set()

    def onItemUpdate(self, update):
        item = update.getItemName()
        received_at = datetime.now(timezone.utc)
        value_raw = update.getValue("Value")
        source_timestamp_raw = update.getValue("TimeStamp")

        record = {
            "received_at_utc": received_at.isoformat(),
            "received_unix_ms": int(received_at.timestamp() * 1000),
            "item": item,
            "value_raw": value_raw,
            "value_numeric": parse_numeric_value(value_raw),
            "source_timestamp_raw": source_timestamp_raw,
            "source": SOURCE_NAME,
        }

        WRITER.write_record(record, received_at)
        MONITOR.note_message(item, received_at)

        with STATS_LOCK:
            ITEM_STATS[item]["total_messages"] += 1
            ITEM_STATS[item]["latest_value"] = value_raw if value_raw is not None else ""


def print_message_counts():
    """Print simple per-item message counts during long runs."""
    runtime_seconds = time.monotonic() - START_MONOTONIC

    with STATS_LOCK:
        snapshot = {
            item: dict(item_stats)
            for item, item_stats in ITEM_STATS.items()
        }

    print()
    print(f"[counts] {utc_now_iso()} runtime={runtime_seconds:.1f}s")
    print(f"{'item':<14} {'messages':>10} {'latest_value':>22}")
    print("-" * 50)

    for item in TELEMETRY_ITEMS:
        latest_value = str(snapshot[item]["latest_value"])
        if len(latest_value) > 22:
            latest_value = latest_value[:19] + "..."

        print(
            f"{item:<14} "
            f"{snapshot[item]['total_messages']:>10} "
            f"{latest_value:>22}"
        )


def counts_loop(interval_seconds):
    """Print counts every so often while the recorder runs."""
    while not STOP_EVENT.wait(interval_seconds):
        print_message_counts()


def handle_shutdown(signum, frame):
    """Stop cleanly when the user presses Ctrl+C or SIGTERM."""
    print("\n[client] shutting down...")
    STOP_EVENT.set()


def parse_args():
    """Allow a small amount of runtime configuration from the CLI."""
    parser = argparse.ArgumentParser(
        description="Collect ISS telemetry into hourly JSONL files."
    )
    parser.add_argument(
        "--counts-interval",
        type=int,
        default=DEFAULT_COUNTS_INTERVAL_SECONDS,
        help="Seconds between console count reports.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    print("[client] connecting to the ISS public Lightstreamer feed...")
    print(f"[client] writing hourly files under {settings.COLLECTOR_RAW_ROOT}/YYYY-MM-DD/")
    print(f"[client] counts interval: {args.counts_interval} seconds")
    write_manifest()

    threading.Thread(
        target=counts_loop,
        args=(args.counts_interval,),
        daemon=True,
    ).start()

    client = None
    subscription = None
    reconnect = False

    try:
        while not STOP_EVENT.is_set():
            try:
                MONITOR.mark_connect_attempt(reconnect=reconnect)
                client = LightstreamerClient("https://push.lightstreamer.com", "ISSLIVE")
                client.addListener(StatusPrinter())

                subscription = Subscription("MERGE", TELEMETRY_ITEMS, TELEMETRY_FIELDS)
                subscription.setRequestedSnapshot("yes")
                subscription.addListener(TelemetryRecorder())

                client.subscribe(subscription)
                client.connect()

                while not STOP_EVENT.wait(1):
                    if MONITOR.check_watchdog() == "reconnect":
                        MONITOR.mark_disconnect(reason="watchdog_forced_reconnect")
                        reconnect = True
                        break
                else:
                    break
            except Exception as exc:
                MONITOR.mark_exception(exc)
                reconnect = True
                if STOP_EVENT.is_set():
                    break
                time.sleep(2)
            finally:
                if client is not None and subscription is not None:
                    try:
                        client.unsubscribe(subscription)
                    except Exception:
                        pass

                if client is not None:
                    try:
                        client.disconnect()
                    except Exception:
                        pass

                client = None
                subscription = None

                if reconnect and not STOP_EVENT.is_set():
                    time.sleep(2)
    finally:
        MONITOR.mark_disconnect(reason="shutdown")

    WRITER.close()

    total_runtime_seconds = time.monotonic() - START_MONOTONIC
    print_message_counts()
    print(f"[client] total runtime seconds: {total_runtime_seconds:.1f}")
    print("[client] disconnected")


if __name__ == "__main__":
    main()
