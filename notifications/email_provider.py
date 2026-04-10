import json
from dataclasses import dataclass
from urllib import error, request

from config.settings import settings


@dataclass
class EmailSendResult:
    provider: str
    message_id: str | None = None


def send_email(*, to: str, subject: str, html: str, text: str) -> EmailSendResult:
    provider = settings.EMAIL_PROVIDER.lower().strip()
    if provider == "resend":
        return _send_via_resend(to=to, subject=subject, html=html, text=text)
    raise ValueError(f"Unsupported email provider '{settings.EMAIL_PROVIDER}'")


def _send_via_resend(*, to: str, subject: str, html: str, text: str) -> EmailSendResult:
    if not settings.RESEND_API_KEY:
        raise ValueError("RESEND_API_KEY is required to send email")
    if not settings.EMAIL_FROM:
        raise ValueError("EMAIL_FROM is required to send email")

    payload = json.dumps(
        {
            "from": settings.EMAIL_FROM,
            "to": [to],
            "subject": subject,
            "html": html,
            "text": text,
        }
    ).encode("utf-8")

    req = request.Request(
        "https://api.resend.com/emails",
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {settings.RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
    )

    try:
        with request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return EmailSendResult(provider="resend", message_id=body.get("id"))
    except error.HTTPError as exc:
        message = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Resend email failed ({exc.code}): {message}") from exc

