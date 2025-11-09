from ingestion.clients import PostgresClient
from dotenv import load_dotenv
import os, sys
from datetime import date, datetime, timedelta

load_dotenv()

pg_schema = os.getenv("PG_SCHEMA")
pg_dsn = os.getenv("PG_DSN")
pklyn_open_date = os.getenv("PKLYN_FIRST_DAY_AFTER_OPENING")

watermark = datetime.strptime(pklyn_open_date, "%Y-%m-%d").date()

args = sys.argv[1:]

if len(args) and args[0] == "test":
    watermark = (datetime.today() - timedelta(weeks=4)).date()

pg_client = PostgresClient(pg_dsn, pg_schema)

with pg_client._connect() as conn, conn.cursor() as cur:
    conn.autocommit = True

    cur.execute(f"TRUNCATE table {pg_schema}.reservation_cancellations_raw")
    cur.execute(f"TRUNCATE table {pg_schema}.reservation_cancellations_raw_stg")
    cur.execute(
        f"""UPDATE {pg_schema}.elt_watermarks
            SET last_loaded_at = %s, 
                last_record_created_at = %s
            WHERE source_name = 'reservation_cancellations'
        """,
        (watermark, watermark),
    )
