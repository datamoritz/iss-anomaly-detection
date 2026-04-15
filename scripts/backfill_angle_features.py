from config.runtime import (
    ANGLE_ITEM_ID,
    backfill_angle_features,
    create_postgres_connection,
    ensure_postgres_schema,
)


def main() -> None:
    conn = create_postgres_connection()
    try:
        ensure_postgres_schema(conn)
        updated_rows = backfill_angle_features(conn)
        print(
            f"[backfill] populated angle features for {updated_rows} "
            f"telemetry_history rows for item {ANGLE_ITEM_ID}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
