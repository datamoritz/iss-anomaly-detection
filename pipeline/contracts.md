## Telemetry event contract

{
  "received_at_utc": "ISO-8601 UTC string",
  "received_unix_ms": 1775520944272,
  "item": "S0000004",
  "value_raw": "225.4248046875",
  "value_numeric": 225.4248046875,
  "source_timestamp_raw": "2328.2615552777715",
  "source": "iss_lightstreamer_public"
}


## Anomaly event contract
{
  "detected_at_utc": "2026-04-07T00:22:10.123456+00:00",
  "item": "S0000004",
  "anomaly_type": "sudden_jump",
  "value_numeric": 225.42,
  "previous_value_numeric": 239.91,
  "threshold": 10.0,
  "details": {
    "delta": 14.49
  },
  "source": "threshold_worker_v1"
}