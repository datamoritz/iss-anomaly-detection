import json
import signal
from typing import Any

from kafka import KafkaConsumer

from config.runtime import create_postgres_connection, ensure_postgres_schema, retry_operation
from config.settings import settings

from .email_provider import send_email
from .templates import build_anomaly_alert_email


running = True


def create_kafka_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        settings.KAFKA_ANOMALY_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=settings.KAFKA_NOTIFICATION_CONSUMER_GROUP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )


def handle_shutdown(signum, frame):
    global running
    print(f"\n[shutdown] received signal {signum}, stopping notification service...")
    running = False


def anomaly_signature(anomaly: dict[str, Any]) -> str:
    return "|".join(
        [
            anomaly.get("item", ""),
            anomaly.get("anomaly_type", ""),
            anomaly.get("detected_at_utc", ""),
            str(anomaly.get("value_numeric")),
            str(anomaly.get("previous_value_numeric")),
            str(anomaly.get("threshold_value")),
        ]
    )


def find_matching_subscriptions(conn, anomaly: dict[str, Any]) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, email, item_id, anomaly_type, cooldown_minutes, unsubscribe_token
            FROM subscriptions
            WHERE enabled = TRUE
              AND is_verified = TRUE
              AND (item_id IS NULL OR item_id = %s)
              AND (anomaly_type IS NULL OR anomaly_type = %s)
            """,
            (anomaly["item"], anomaly["anomaly_type"]),
        )
        return cur.fetchall()


def already_sent_for_signature(conn, subscription_id: int, signature: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM notification_log
            WHERE subscription_id = %s
              AND anomaly_signature = %s
              AND status = 'sent'
            LIMIT 1
            """,
            (subscription_id, signature),
        )
        return cur.fetchone() is not None


def cooldown_active(conn, subscription_id: int, item: str, anomaly_type: str, cooldown_minutes: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM notification_log
            WHERE subscription_id = %s
              AND item = %s
              AND anomaly_type = %s
              AND status = 'sent'
              AND sent_at >= (NOW() - (%s * INTERVAL '1 minute'))
            LIMIT 1
            """,
            (subscription_id, item, anomaly_type, cooldown_minutes),
        )
        return cur.fetchone() is not None


def log_notification(
    conn,
    *,
    subscription_id: int,
    anomaly: dict[str, Any],
    signature: str,
    status: str,
    provider: str | None,
    provider_message_id: str | None,
    error_message: str | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO notification_log (
                subscription_id,
                anomaly_signature,
                item,
                anomaly_type,
                detected_at_utc,
                status,
                provider,
                provider_message_id,
                error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (subscription_id, anomaly_signature) DO UPDATE
            SET status = EXCLUDED.status,
                provider = EXCLUDED.provider,
                provider_message_id = EXCLUDED.provider_message_id,
                error_message = EXCLUDED.error_message,
                sent_at = NOW()
            """,
            (
                subscription_id,
                signature,
                anomaly["item"],
                anomaly["anomaly_type"],
                anomaly.get("detected_at_utc"),
                status,
                provider,
                provider_message_id,
                error_message,
            ),
        )
    conn.commit()


def update_subscription_last_sent(conn, subscription_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE subscriptions
            SET last_sent_at = NOW()
            WHERE id = %s
            """,
            (subscription_id,),
        )
    conn.commit()


def process_anomaly(conn, anomaly: dict[str, Any]) -> int:
    sent_count = 0
    signature = anomaly_signature(anomaly)
    subscriptions = find_matching_subscriptions(conn, anomaly)

    for subscription_id, email, _, _, cooldown_minutes, unsubscribe_token in subscriptions:
        if already_sent_for_signature(conn, subscription_id, signature):
            continue

        effective_cooldown = cooldown_minutes or settings.DEFAULT_NOTIFICATION_COOLDOWN_MINUTES
        if cooldown_active(
            conn,
            subscription_id,
            anomaly["item"],
            anomaly["anomaly_type"],
            effective_cooldown,
        ):
            continue

        unsubscribe_url = (
            f"{settings.APP_BASE_URL.rstrip('/')}"
            f"/api/v1/subscriptions/unsubscribe?token={unsubscribe_token}"
        )
        subject, text, html = build_anomaly_alert_email(
            anomaly=anomaly,
            unsubscribe_url=unsubscribe_url,
        )

        try:
            result = send_email(to=email, subject=subject, text=text, html=html)
            log_notification(
                conn,
                subscription_id=subscription_id,
                anomaly=anomaly,
                signature=signature,
                status="sent",
                provider=result.provider,
                provider_message_id=result.message_id,
                error_message=None,
            )
            update_subscription_last_sent(conn, subscription_id)
            sent_count += 1
        except Exception as exc:
            log_notification(
                conn,
                subscription_id=subscription_id,
                anomaly=anomaly,
                signature=signature,
                status="failed",
                provider=settings.EMAIL_PROVIDER,
                provider_message_id=None,
                error_message=str(exc),
            )
            print(f"[notify] failed subscription={subscription_id}: {exc}")

    return sent_count


def main() -> None:
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    print("[startup] connecting to Postgres...")
    pg_conn = retry_operation(
        "notifications postgres startup",
        create_postgres_connection,
    )
    retry_operation(
        "notifications schema initialization",
        lambda: ensure_postgres_schema(pg_conn),
    )
    print("[startup] Postgres connected and tables ensured")

    print("[startup] creating Kafka consumer...")
    consumer = retry_operation(
        "notifications kafka consumer startup",
        create_kafka_consumer,
    )
    print(f"[startup] notification consumer is running topic={settings.KAFKA_ANOMALY_TOPIC}")

    processed = 0
    sent = 0

    try:
        while running:
            records = consumer.poll(timeout_ms=1000)
            for _, messages in records.items():
                for message in messages:
                    anomaly = message.value
                    sent += process_anomaly(pg_conn, anomaly)
                    processed += 1
                    if processed % 25 == 0:
                        print(f"[notify] processed={processed} sent={sent}")
    finally:
        print("[shutdown] closing Kafka consumer...")
        try:
            consumer.close()
        except Exception:
            pass
        print("[shutdown] closing Postgres...")
        try:
            pg_conn.close()
        except Exception:
            pass
        print("[shutdown] done")


if __name__ == "__main__":
    main()
