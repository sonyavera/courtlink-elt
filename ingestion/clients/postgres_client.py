import psycopg2
from psycopg2.extras import execute_values
from datetime import date, datetime, timezone
from typing import Optional, Tuple
from constants import Tables, EltWatermarks
import json
import logging
import os
from .postgres_mixins import DedupeMixin, InsertMixin

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logs_dir = os.path.join(BASE_DIR, "logs")
os.makedirs(logs_dir, exist_ok=True)

log_path = os.path.join(logs_dir, "dedupe.log")

logging.basicConfig(
    filename=log_path,
    filemode="a",
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


class PostgresClient(DedupeMixin, InsertMixin):
    def __init__(self, dsn, schema):
        self.dsn = dsn
        self.schema = schema

    def _connect(self):
        dsn = self.dsn
        return psycopg2.connect(dsn)

    def get_current_member_count(self):
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT count(*) from {self.schema}.members_raw")
            count = cur.fetchone()

        if count and count[0]:
            return count[0]
        else:
            return None

    def get_elt_watermark(
        self, source_name: str
    ) -> Optional[Tuple[Optional[datetime], Optional[datetime]]]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT last_loaded_at, last_record_created_at 
                  FROM "{self.schema}".{Tables.ELT_WATERMARKS} WHERE source_name = %s
                """,
                (source_name,),
            )
            row = cur.fetchone()

        if row:
            return row
        return None

    def update_elt_watermark(
        self, source_name: str, last_record_created_at: Optional[datetime] = None
    ) -> None:
        now_ts = datetime.now(timezone.utc)

        if last_record_created_at:
            query = f"""
                INSERT INTO "{self.schema}"."{Tables.ELT_WATERMARKS}" (
                    source_name,
                    last_loaded_at,
                    last_record_created_at
                ) VALUES (%s, %s, %s)
                ON CONFLICT (source_name) DO UPDATE
                SET last_loaded_at = EXCLUDED.last_loaded_at,
                    last_record_created_at = EXCLUDED.last_record_created_at
            """
            params = (source_name, now_ts, last_record_created_at)
        else:
            query = f"""
                INSERT INTO "{self.schema}"."{Tables.ELT_WATERMARKS}" (
                    source_name,
                    last_loaded_at
                ) VALUES (%s, %s)
                ON CONFLICT (source_name) DO UPDATE
                SET last_loaded_at = EXCLUDED.last_loaded_at
            """
            params = (source_name, now_ts)

        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(query, params)

    def truncate_table(self, table_name):
        """Truncate a table in the given schema."""
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f'TRUNCATE TABLE "{self.schema}"."{table_name}"')

    def delete_members_for_client(self, client_code: str) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f'DELETE FROM "{self.schema}"."{Tables.MEMBERS_RAW}" WHERE client_code = %s',
                (client_code.lower(),),
            )

    def delete_reservations_for_client(self, client_code: str) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f'DELETE FROM "{self.schema}"."{Tables.RESERVATIONS_RAW}" WHERE client_code = %s',
                (client_code.lower(),),
            )
            cur.execute(
                f'DELETE FROM "{self.schema}"."{Tables.RESERVATIONS_RAW_STG}" WHERE client_code = %s',
                (client_code.lower(),),
            )

    def delete_reservations_for_ids(
        self, client_code: str, reservation_ids: list[str]
    ) -> None:
        if not reservation_ids:
            return

        normalized_ids = [
            str(reservation_id)
            for reservation_id in reservation_ids
            if reservation_id is not None and str(reservation_id).strip()
        ]

        if not normalized_ids:
            return

        client_code = client_code.lower()

        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f'''
                DELETE FROM "{self.schema}"."{Tables.RESERVATIONS_RAW}"
                WHERE client_code = %s
                  AND reservation_id = ANY(%s)
                ''',
                (client_code, normalized_ids),
            )
            cur.execute(
                f'''
                DELETE FROM "{self.schema}"."{Tables.RESERVATIONS_RAW_STG}"
                WHERE client_code = %s
                  AND reservation_id = ANY(%s)
                ''',
                (client_code, normalized_ids),
            )

    def delete_members_stg_for_client(self, client_code: str) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f'DELETE FROM "{self.schema}"."{Tables.MEMBERS_RAW_STG}" WHERE client_code = %s',
                (client_code.lower(),),
            )

    def replace_members_for_client(self, client_code: str, members: list[dict]) -> None:
        client_code = client_code.lower()
        print(f"[REPLACE MEMBERS] Starting for client_code={client_code} with {len(members)} members")
        
        # Step 1: Clear STG table
        print(f"[REPLACE MEMBERS] Step 1: Clearing STG table for {client_code}")
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f'SELECT COUNT(*) FROM "{self.schema}"."{Tables.MEMBERS_RAW_STG}" WHERE client_code = %s',
                (client_code,),
            )
            stg_count_before = cur.fetchone()[0]
        
        self.delete_members_stg_for_client(client_code)
        print(f"[REPLACE MEMBERS] Deleted {stg_count_before} existing records from STG")
        
        # Step 2: Insert into STG
        print(f"[REPLACE MEMBERS] Step 2: Inserting {len(members)} members into STG")
        self.insert_members(members, Tables.MEMBERS_RAW_STG)
        
        # Step 3: Check STG count before moving to PROD
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f'SELECT COUNT(*) FROM "{self.schema}"."{Tables.MEMBERS_RAW_STG}" WHERE client_code = %s',
                (client_code,),
            )
            stg_count_after_insert = cur.fetchone()[0]
            print(f"[REPLACE MEMBERS] STG table now has {stg_count_after_insert} records")
            
            # Check PROD count before insert
            cur.execute(
                f'SELECT COUNT(*) FROM "{self.schema}"."{Tables.MEMBERS_RAW}" WHERE client_code = %s',
                (client_code,),
            )
            prod_count_before = cur.fetchone()[0]
            print(f"[REPLACE MEMBERS] PROD table currently has {prod_count_before} records for {client_code}")
        
        # Step 4: Move from STG to PROD (with deduplication via ON CONFLICT)
        print(f"[REPLACE MEMBERS] Step 3: Moving records from STG to PROD (deduplication via ON CONFLICT)")
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO "{self.schema}"."{Tables.MEMBERS_RAW}" (
                    client_code,
                    member_id,
                    first_name,
                    last_name,
                    gender,
                    date_of_birth,
                    email,
                    phone_number,
                    membership_type_name,
                    is_premium_member,
                    member_since,
                    created_at
                )
                SELECT
                    client_code,
                    member_id,
                    first_name,
                    last_name,
                    gender,
                    date_of_birth,
                    email,
                    phone_number,
                    membership_type_name,
                    is_premium_member,
                    member_since,
                    created_at
                FROM "{self.schema}"."{Tables.MEMBERS_RAW_STG}"
                WHERE client_code = %s
                ON CONFLICT (client_code, member_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    gender = EXCLUDED.gender,
                    date_of_birth = EXCLUDED.date_of_birth,
                    email = EXCLUDED.email,
                    phone_number = EXCLUDED.phone_number,
                    membership_type_name = EXCLUDED.membership_type_name,
                    is_premium_member = EXCLUDED.is_premium_member,
                    member_since = EXCLUDED.member_since,
                    created_at = EXCLUDED.created_at
                """,
                (client_code,),
            )
            rows_affected = cur.rowcount
            print(f"[REPLACE MEMBERS] PROD insert/update complete: {rows_affected} rows affected")
            
            # Check PROD count after insert
            cur.execute(
                f'SELECT COUNT(*) FROM "{self.schema}"."{Tables.MEMBERS_RAW}" WHERE client_code = %s',
                (client_code,),
            )
            prod_count_after = cur.fetchone()[0]
            print(
                f"[REPLACE MEMBERS] PROD table now has {prod_count_after} records "
                f"(was {prod_count_before}, change: {prod_count_after - prod_count_before})"
            )
            
            # Calculate new vs updated
            # Since we can't easily distinguish inserts from updates with ON CONFLICT,
            # we'll estimate: if prod_count increased, those are new; otherwise mostly updates
            if prod_count_after > prod_count_before:
                estimated_new = prod_count_after - prod_count_before
                estimated_updates = rows_affected - estimated_new
                print(
                    f"[REPLACE MEMBERS] Estimated: ~{estimated_new} new records, "
                    f"~{estimated_updates} updated records"
                )
            else:
                print(f"[REPLACE MEMBERS] All {rows_affected} records were updates (no new records)")

        # Step 5: Clean up STG
        print(f"[REPLACE MEMBERS] Step 4: Cleaning up STG table")
        self.delete_members_stg_for_client(client_code)
        
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f'SELECT COUNT(*) FROM "{self.schema}"."{Tables.MEMBERS_RAW_STG}" WHERE client_code = %s',
                (client_code,),
            )
            stg_count_final = cur.fetchone()[0]
            print(f"[REPLACE MEMBERS] STG cleanup complete: {stg_count_final} records remaining")
        
        print(f"[REPLACE MEMBERS] Complete for {client_code}: {rows_affected} total rows processed")

    # Methods for inserts/dedupe/cleanup are inherited from mixins
