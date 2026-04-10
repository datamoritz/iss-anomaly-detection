from secrets import token_urlsafe

from fastapi import HTTPException

from config.runtime import create_postgres_connection
from config.settings import settings
from notifications.email_provider import send_email
from notifications.templates import build_verification_email

from ..schemas import SubscriptionCreateRequest


def _public_url(path: str, token: str) -> str:
    base = settings.APP_BASE_URL.rstrip("/")
    return f"{base}{path}?token={token}"


def _normalize_scope(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def create_subscription(request: SubscriptionCreateRequest) -> int:
    email = request.email.strip().lower()
    item_id = _normalize_scope(request.item_id)
    anomaly_type = _normalize_scope(request.anomaly_type)
    verify_token = token_urlsafe(32)
    unsubscribe_token = token_urlsafe(32)
    conn = create_postgres_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM subscriptions
                WHERE lower(email) = lower(%s)
                  AND item_id IS NOT DISTINCT FROM %s
                  AND anomaly_type IS NOT DISTINCT FROM %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (email, item_id, anomaly_type),
            )
            row = cur.fetchone()

            if row is None:
                cur.execute(
                    """
                    INSERT INTO subscriptions (
                        email,
                        item_id,
                        anomaly_type,
                        enabled,
                        is_verified,
                        verify_token,
                        unsubscribe_token,
                        cooldown_minutes
                    )
                    VALUES (%s, %s, %s, TRUE, FALSE, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        email,
                        item_id,
                        anomaly_type,
                        verify_token,
                        unsubscribe_token,
                        settings.DEFAULT_NOTIFICATION_COOLDOWN_MINUTES,
                    ),
                )
                subscription_id = cur.fetchone()[0]
            else:
                subscription_id = row[0]
                cur.execute(
                    """
                    UPDATE subscriptions
                    SET enabled = TRUE,
                        is_verified = FALSE,
                        verify_token = %s,
                        unsubscribe_token = %s,
                        cooldown_minutes = COALESCE(cooldown_minutes, %s),
                        verified_at = NULL
                    WHERE id = %s
                    """,
                    (
                        verify_token,
                        unsubscribe_token,
                        settings.DEFAULT_NOTIFICATION_COOLDOWN_MINUTES,
                        subscription_id,
                    ),
                )
        conn.commit()
    finally:
        conn.close()

    verify_url = _public_url("/api/v1/subscriptions/verify", verify_token)
    unsubscribe_url = _public_url("/api/v1/subscriptions/unsubscribe", unsubscribe_token)
    subject, text, html = build_verification_email(
        verify_url=verify_url,
        unsubscribe_url=unsubscribe_url,
    )
    send_email(to=email, subject=subject, text=text, html=html)
    return subscription_id


def verify_subscription(token: str) -> tuple[bool, int | None]:
    conn = create_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, is_verified
                FROM subscriptions
                WHERE verify_token = %s
                LIMIT 1
                """,
                (token,),
            )
            row = cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Verification token not found")

            subscription_id, is_verified = row
            if not is_verified:
                cur.execute(
                    """
                    UPDATE subscriptions
                    SET is_verified = TRUE,
                        verify_token = NULL,
                        verified_at = NOW()
                    WHERE id = %s
                    """,
                    (subscription_id,),
                )
                conn.commit()
                return True, subscription_id

            return False, subscription_id
    finally:
        conn.close()


def unsubscribe_by_token(token: str) -> int:
    conn = create_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE subscriptions
                SET enabled = FALSE
                WHERE unsubscribe_token = %s
                RETURNING id
                """,
                (token,),
            )
            row = cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Unsubscribe token not found")
            conn.commit()
            return row[0]
    finally:
        conn.close()


def unsubscribe_by_id(subscription_id: int, token: str) -> None:
    conn = create_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE subscriptions
                SET enabled = FALSE
                WHERE id = %s
                  AND unsubscribe_token = %s
                RETURNING id
                """,
                (subscription_id, token),
            )
            row = cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Subscription/token pair not found")
            conn.commit()
    finally:
        conn.close()
