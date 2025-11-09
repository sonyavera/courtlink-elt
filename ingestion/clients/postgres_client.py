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

    def delete_members_stg_for_client(self, client_code: str) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f'DELETE FROM "{self.schema}"."{Tables.MEMBERS_RAW_STG}" WHERE client_code = %s',
                (client_code.lower(),),
            )

    def replace_members_for_client(self, client_code: str, members: list[dict]) -> None:
        client_code = client_code.lower()
        self.delete_members_stg_for_client(client_code)
        self.insert_members(members, Tables.MEMBERS_RAW_STG)
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
                    created_at = EXCLUDED.created_at
                """,
                (client_code,),
            )

        self.delete_members_stg_for_client(client_code)

    # Methods for inserts/dedupe/cleanup are inherited from mixins
