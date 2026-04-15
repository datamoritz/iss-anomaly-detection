import json
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_jsonl(path: str | Path, payload: dict) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


class StreamMonitor:
    def __init__(
        self,
        *,
        stream_name: str,
        selected_items: list[str],
        diagnostics_path: str | Path,
        gap_threshold_seconds: int,
        heartbeat_warn_seconds: int,
        heartbeat_reconnect_seconds: int,
    ) -> None:
        self.stream_name = stream_name
        self.selected_items = list(selected_items)
        self.diagnostics_path = diagnostics_path
        self.gap_threshold_seconds = gap_threshold_seconds
        self.heartbeat_warn_seconds = heartbeat_warn_seconds
        self.heartbeat_reconnect_seconds = heartbeat_reconnect_seconds
        self.last_message_monotonic: float | None = None
        self.last_message_received_at: datetime | None = None
        self.last_seen_per_item = {item: None for item in self.selected_items}
        self.connection_started_monotonic: float | None = None
        self.connection_started_at_utc: str | None = None
        self.awaiting_first_message = False
        self.warned_stale = False
        self.connection_attempt_count = 0

    def log_event(self, event: str, **details) -> None:
        payload = {
            "stream": self.stream_name,
            "event": event,
            "at_utc": utc_now_iso(),
            **details,
        }
        print(f"[{self.stream_name}] {json.dumps(payload, ensure_ascii=True)}")

    def mark_connect_attempt(self, reconnect: bool) -> None:
        self.connection_attempt_count += 1
        self.connection_started_monotonic = time.monotonic()
        self.connection_started_at_utc = utc_now_iso()
        self.awaiting_first_message = True
        self.warned_stale = False
        self.log_event(
            "reconnect_attempt" if reconnect else "connect_attempt",
            attempt=self.connection_attempt_count,
            reconnect=reconnect,
            last_message_utc=(
                self.last_message_received_at.isoformat()
                if self.last_message_received_at
                else None
            ),
        )

    def mark_connected(self, status: str | None = None) -> None:
        self.log_event(
            "connected",
            status=status,
            attempt=self.connection_attempt_count,
        )

    def mark_subscribed(self) -> None:
        self.log_event(
            "subscribed",
            attempt=self.connection_attempt_count,
        )

    def mark_disconnect(self, reason: str) -> None:
        self.log_event(
            "disconnect",
            reason=reason,
            last_message_utc=(
                self.last_message_received_at.isoformat()
                if self.last_message_received_at
                else None
            ),
        )

    def mark_exception(self, exc: Exception) -> None:
        self.log_event(
            "exception",
            error=str(exc),
            traceback=traceback.format_exc(),
            last_message_utc=(
                self.last_message_received_at.isoformat()
                if self.last_message_received_at
                else None
            ),
        )

    def note_message(self, item: str, received_at: datetime) -> None:
        gap_seconds = None
        affected_items: list[str] = []

        if self.awaiting_first_message:
            event_name = (
                "first_message_after_reconnect"
                if self.connection_attempt_count > 1
                else "first_message_after_connect"
            )
            self.log_event(
                event_name,
                item=item,
                received_at_utc=received_at.isoformat(),
                first_after_attempt=self.connection_attempt_count,
            )
            if self.connection_attempt_count > 1:
                self.log_event(
                    "reconnect_success",
                    attempt=self.connection_attempt_count,
                    first_message_item=item,
                    first_message_utc=received_at.isoformat(),
                )
            self.awaiting_first_message = False

        if self.last_message_received_at is not None:
            gap_seconds = (received_at - self.last_message_received_at).total_seconds()
            if gap_seconds > self.gap_threshold_seconds:
                affected_items = sorted(
                    item_id
                    for item_id, last_seen in self.last_seen_per_item.items()
                    if last_seen is None
                    or (received_at - last_seen).total_seconds() > self.gap_threshold_seconds
                )
                diagnostic = {
                    "stream": self.stream_name,
                    "event": "gap_detected",
                    "detected_at_utc": utc_now_iso(),
                    "gap_start_utc": self.last_message_received_at.isoformat(),
                    "gap_end_utc": received_at.isoformat(),
                    "gap_seconds": gap_seconds,
                    "affected_parameter_count": len(affected_items),
                    "affected_items": affected_items,
                    "resume_item": item,
                }
                append_jsonl(self.diagnostics_path, diagnostic)
                self.log_event("gap_detected", **diagnostic)

        self.last_message_monotonic = time.monotonic()
        self.last_message_received_at = received_at
        self.last_seen_per_item[item] = received_at
        self.warned_stale = False

    def check_watchdog(self) -> str | None:
        reference = self.last_message_monotonic
        if reference is None:
            reference = self.connection_started_monotonic
        if reference is None:
            return None

        idle_seconds = time.monotonic() - reference

        if idle_seconds > self.heartbeat_reconnect_seconds:
            self.log_event(
                "watchdog_reconnect",
                idle_seconds=round(idle_seconds, 3),
                threshold_seconds=self.heartbeat_reconnect_seconds,
                last_message_utc=(
                    self.last_message_received_at.isoformat()
                    if self.last_message_received_at
                    else None
                ),
            )
            self.warned_stale = True
            return "reconnect"

        if idle_seconds > self.heartbeat_warn_seconds and not self.warned_stale:
            self.log_event(
                "watchdog_warning",
                idle_seconds=round(idle_seconds, 3),
                threshold_seconds=self.heartbeat_warn_seconds,
                last_message_utc=(
                    self.last_message_received_at.isoformat()
                    if self.last_message_received_at
                    else None
                ),
            )
            self.warned_stale = True

        return None
