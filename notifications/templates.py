from config.items import ITEM_METADATA


ITEM_LABELS = {item["item"]: item["label"] for item in ITEM_METADATA}


def format_item_display(item_id: str) -> str:
    label = ITEM_LABELS.get(item_id)
    if label:
        return f"{label} ({item_id})"
    return item_id


def build_verification_email(*, verify_url: str, unsubscribe_url: str) -> tuple[str, str, str]:
    subject = "Verify your ISS anomaly alerts"
    text = (
        "Verify your ISS anomaly alert subscription.\n\n"
        f"Verify: {verify_url}\n"
        f"Unsubscribe: {unsubscribe_url}\n"
    )
    html = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #111827; line-height: 1.5;">
        <h2>Verify your ISS anomaly alerts</h2>
        <p>Confirm this email address to start receiving anomaly notifications.</p>
        <p><a href="{verify_url}">Verify subscription</a></p>
        <p style="font-size: 13px; color: #6b7280;">If this was not you, you can ignore this email or unsubscribe here: <a href="{unsubscribe_url}">unsubscribe</a>.</p>
      </body>
    </html>
    """.strip()
    return subject, text, html


def build_anomaly_alert_email(*, anomaly: dict, unsubscribe_url: str) -> tuple[str, str, str]:
    item_display = format_item_display(anomaly["item"])
    details_summary = ", ".join(
        f"{key}={value}" for key, value in sorted((anomaly.get("details") or {}).items())
    ) or "none"
    subject = f"ISS anomaly detected: {item_display} ({anomaly['anomaly_type']})"
    text = (
        f"Anomaly detected for {item_display}.\n\n"
        f"Type: {anomaly['anomaly_type']}\n"
        f"Detected: {anomaly['detected_at_utc']}\n"
        f"Current value: {anomaly.get('value_numeric')}\n"
        f"Previous value: {anomaly.get('previous_value_numeric')}\n"
        f"Threshold: {anomaly.get('threshold_value')}\n"
        f"Source: {anomaly.get('source')}\n"
        f"Details: {details_summary}\n\n"
        f"Unsubscribe: {unsubscribe_url}\n"
    )
    html = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #111827; line-height: 1.5;">
        <h2>ISS anomaly detected</h2>
        <p><strong>Item:</strong> {item_display}</p>
        <p><strong>Type:</strong> {anomaly['anomaly_type']}</p>
        <p><strong>Detected:</strong> {anomaly['detected_at_utc']}</p>
        <p><strong>Current value:</strong> {anomaly.get('value_numeric')}</p>
        <p><strong>Previous value:</strong> {anomaly.get('previous_value_numeric')}</p>
        <p><strong>Threshold:</strong> {anomaly.get('threshold_value')}</p>
        <p><strong>Source:</strong> {anomaly.get('source')}</p>
        <p><strong>Details:</strong> {details_summary}</p>
        <p style="font-size: 13px; color: #6b7280;"><a href="{unsubscribe_url}">Unsubscribe</a></p>
      </body>
    </html>
    """.strip()
    return subject, text, html
