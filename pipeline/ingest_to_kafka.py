import json
import signal
import time
from datetime import datetime, timezone
from typing import Optional

from config.settings import settings
from config.stream_monitoring import StreamMonitor

from kafka import KafkaProducer
from lightstreamer.client import LightstreamerClient, Subscription
from lightstreamer.client.ls_python_client_api import ItemUpdate


LIGHTSTREAMER_SERVER = settings.LIGHTSTREAMER_SERVER
LIGHTSTREAMER_ADAPTER_SET = settings.LIGHTSTREAMER_ADAPTER_SET
KAFKA_BOOTSTRAP_SERVERS = settings.KAFKA_BOOTSTRAP_SERVERS
KAFKA_TOPIC = settings.KAFKA_TELEMETRY_TOPIC

SELECTED_ITEMS = [
    "S0000004",      # Port SARJ angle position
    "NODE3000012",   # Avionics Cooling Fluid Temp (Node 3)
    "P1000003",      # Loop B PM Out Temp
    "NODE3000013",   # Air Cooling Fluid Temp (Node 3)
    "S1000003",      # Loop A PM Out Temp
    "USLAB000059",   # Cabin Temperature
    "NODE3000005",   # Urine Tank [%]
    "NODE3000009",   # Clean Water Tank
]

# The public ISS feed exposes field values like value + timestamp.
# This matches the pattern you already used in your collector.
SUBSCRIPTION_FIELDS = ["Value", "TimeStamp"]


running = True
message_count = 0
MONITOR = StreamMonitor(
    stream_name="app_ingest",
    selected_items=SELECTED_ITEMS,
    diagnostics_path=settings.APP_INGEST_DIAGNOSTICS_PATH,
    gap_threshold_seconds=settings.APP_INGEST_GAP_THRESHOLD_SECONDS,
    heartbeat_warn_seconds=settings.APP_INGEST_HEARTBEAT_WARN_SECONDS,
    heartbeat_reconnect_seconds=settings.APP_INGEST_HEARTBEAT_RECONNECT_SECONDS,
)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_unix_ms() -> int:
    return int(time.time() * 1000)


def parse_numeric(value_raw: Optional[str]) -> Optional[float]:
    if value_raw is None:
        return None
    try:
        return float(value_raw)
    except (ValueError, TypeError):
        return None


def build_event(item: str, value_raw: Optional[str], source_timestamp_raw: Optional[str]) -> dict:
    return {
        "received_at_utc": now_utc_iso(),
        "received_unix_ms": now_unix_ms(),
        "item": item,
        "value_raw": value_raw,
        "value_numeric": parse_numeric(value_raw),
        "source_timestamp_raw": source_timestamp_raw,
        "source": "iss_lightstreamer_public",
    }


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )


class ClientListener:
    def onListenStart(self):
        print("[client] listener started")

    def onListenEnd(self):
        print("[client] listener ended")

    def onStatusChange(self, status: str):
        print(f"[client] status: {status}")
        lowered = status.lower()
        if "disconnected" in lowered:
            MONITOR.mark_disconnect(reason=status)
        elif "connected" in lowered:
            MONITOR.mark_connected(status)

    def onServerError(self, code: int, message: str):
        print(f"[client] server error {code}: {message}")
        MONITOR.log_event(
            "server_error",
            error_code=code,
            error_message=message,
        )


class TelemetrySubscriptionListener:
    def __init__(self, producer: KafkaProducer):
        self.producer = producer
        self.per_item_counts = {item: 0 for item in SELECTED_ITEMS}

    def onSubscription(self):
        print("[subscription] subscribed successfully")
        MONITOR.mark_subscribed()

    def onUnsubscription(self):
        print("[subscription] unsubscribed")

    def onItemUpdate(self, update: ItemUpdate):
        global message_count, running

        if not running:
            return

        try:
            item_name = update.getItemName()
            value_raw = update.getValue("Value")
            source_timestamp_raw = update.getValue("TimeStamp")

            event = build_event(
                item=item_name,
                value_raw=value_raw,
                source_timestamp_raw=source_timestamp_raw,
            )

            self.producer.send(
                KAFKA_TOPIC,
                key=item_name,
                value=event,
            )
            MONITOR.note_message(item_name, received_at=received_at_from_event(event))

            message_count += 1
            self.per_item_counts[item_name] = self.per_item_counts.get(item_name, 0) + 1

            if message_count % 100 == 0:
                print(f"[kafka] published {message_count} messages total")
                print("[counts]", self.per_item_counts)

        except Exception as exc:
            print(f"[error] failed to process update: {exc}")
            MONITOR.mark_exception(exc)

    def onSubscriptionError(self, code: int, message: str):
        print(f"[subscription] error {code}: {message}")
        MONITOR.log_event("subscription_error", code=code, message=message)


def handle_shutdown(signum, frame):
    global running
    print(f"\n[shutdown] received signal {signum}, stopping...")
    running = False


def received_at_from_event(event: dict) -> datetime:
    return datetime.fromisoformat(event["received_at_utc"])


def main():
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    print("[startup] creating Kafka producer...")
    producer = create_producer()

    print("[startup] ingest_to_kafka is running")
    print(f"[startup] topic={KAFKA_TOPIC}")
    print(f"[startup] items={SELECTED_ITEMS}")

    client = None
    subscription = None
    reconnect = False

    try:
        while running:
            try:
                MONITOR.mark_connect_attempt(reconnect=reconnect)
                client = LightstreamerClient(LIGHTSTREAMER_SERVER, LIGHTSTREAMER_ADAPTER_SET)
                client.addListener(ClientListener())

                subscription = Subscription(
                    mode="MERGE",
                    items=SELECTED_ITEMS,
                    fields=SUBSCRIPTION_FIELDS,
                )
                subscription.addListener(TelemetrySubscriptionListener(producer))

                client.connect()
                client.subscribe(subscription)

                while running:
                    time.sleep(1)
                    if MONITOR.check_watchdog() == "reconnect":
                        MONITOR.mark_disconnect(reason="watchdog_forced_reconnect")
                        reconnect = True
                        break

                if not running:
                    break
            except Exception as exc:
                MONITOR.mark_exception(exc)
                reconnect = True
                if not running:
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

                if reconnect and running:
                    time.sleep(2)
    finally:
        print("[shutdown] flushing Kafka producer...")
        try:
            producer.flush(timeout=10)
            producer.close()
        except Exception as exc:
            print(f"[shutdown] producer close error: {exc}")

        print("[shutdown] disconnecting Lightstreamer client...")
        MONITOR.mark_disconnect(reason="shutdown")

        print("[shutdown] done")


if __name__ == "__main__":
    main()
