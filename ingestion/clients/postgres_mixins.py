from datetime import date, datetime, timezone
from constants import Tables, EltWatermarks
from psycopg2.extras import execute_values
import json


class DedupeMixin:
    def dedupe_reservation_records(self, stg_table: str, prod_table: str):
        print(f"[DEDUPE] Starting deduplication for {stg_table} → {prod_table}")

        with self._connect() as conn, conn.cursor() as cur:
            # Check count before dedupe
            cur.execute(f'SELECT COUNT(*) FROM "{self.schema}"."{stg_table}"')
            count_before = cur.fetchone()[0]
            print(f"[DEDUPE] STG records before dedupe: {count_before}")

            # 1. Remove duplicates inside staging
            cur.execute(
                f"""
                DELETE FROM "{self.schema}"."{stg_table}" a
                USING (
                    SELECT ctid
                    FROM (
                        SELECT
                            ctid,
                            ROW_NUMBER() OVER (
                                PARTITION BY event_id,
                                            reservation_start_at,
                                            reservation_created_at,
                                            member_id
                                ORDER BY reservation_updated_at DESC NULLS LAST,
                                        created_at DESC
                            ) AS rn
                        FROM "{self.schema}"."{stg_table}"
                    ) t
                    WHERE rn > 1
                ) b
                WHERE a.ctid = b.ctid
                """
            )
            duplicates_removed = cur.rowcount
            print(f"[DEDUPE] Removed {duplicates_removed} duplicate records within STG")

            # 2. Remove anything in staging that also exists in prod
            cur.execute(
                f"""
                DELETE FROM "{self.schema}"."{stg_table}" stg
                USING "{self.schema}"."{prod_table}" prod
                WHERE (
                    stg.event_id,
                    stg.reservation_start_at,
                    stg.reservation_created_at,
                    stg.member_id
                ) IS NOT DISTINCT FROM (
                    prod.event_id,
                    prod.reservation_start_at,
                    prod.reservation_created_at,
                    prod.member_id
                )
                """
            )
            existing_removed = cur.rowcount
            print(
                f"[DEDUPE] Removed {existing_removed} records that already exist in PROD"
            )

            # Check count after dedupe
            cur.execute(f'SELECT COUNT(*) FROM "{self.schema}"."{stg_table}"')
            count_after = cur.fetchone()[0]
            print(
                f"[DEDUPE] STG records after dedupe: {count_after} "
                f"(removed {count_before - count_after} total)"
            )

    def dedupe_reservation_cancellation_records(self, stg_table: str, prod_table: str):
        # 1. Remove duplicates inside staging
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                DELETE FROM "{self.schema}"."{stg_table}" a
                USING (
                    SELECT ctid
                    FROM (
                    SELECT
                        ctid,
                        ROW_NUMBER() OVER (
                            PARTITION BY client_code,
                                        event_id,
                                        reservation_start_at,
                                        cancelled_on,
                                        member_id
                                ORDER BY cancelled_on DESC NULLS LAST,
                                        created_at DESC
                            ) AS rn
                        FROM "{self.schema}"."{stg_table}"
                    ) t
                    WHERE rn > 1
                ) b
                WHERE a.ctid = b.ctid
                """
            )

            # 2. Remove anything in staging that also exists in prod
            cur.execute(
                f"""
                DELETE FROM "{self.schema}"."{stg_table}" stg
                USING "{self.schema}"."{prod_table}" prod
                WHERE (
                    stg.client_code,
                    stg.event_id,
                    stg.reservation_start_at,
                    stg.cancelled_on,
                    stg.member_id
                ) IS NOT DISTINCT FROM (
                    prod.client_code,
                    prod.event_id,
                    prod.reservation_start_at,
                    prod.cancelled_on,
                    prod.member_id
                )
                """
            )

    def remove_records_from_before_timestamp(
        self,
        stg_table: str,
        watermark: str,
    ):
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{self.schema}"."{stg_table}"')
            total_before = cur.fetchone()[0]

            cur.execute(
                f"""
                DELETE FROM "{self.schema}"."{stg_table}"
                WHERE reservation_created_at <= %s
                """,
                (watermark,),
            )
            count_deleted = cur.rowcount
            print(
                f"Deleted {count_deleted} rows out of {total_before}, {total_before - count_deleted} remaining"
            )

    def dedupe_on_event_id_and_program_datetime(self, stg_table: str, prod_table: str):
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {self.schema}.{stg_table}")
            num_rows = cur.fetchone()[0]
            print(f"NUM ROWS IN STG: {num_rows}")

            # 🔍 Find event_ids in stg table with duplicates
            cur.execute(
                f"""
                SELECT event_id, program_date_time, COUNT(*) 
                FROM {self.schema}.{stg_table}
                GROUP BY 1, 2
                HAVING COUNT(*) > 1
                LIMIT 10
                """
            )

            duplicates = cur.fetchall()

            for event_id, program_date_time, count in duplicates:
                print(
                    f"{count} records for event {event_id} with timestamp {program_date_time}, cleaning up"
                )

            # Delete all but one matching row in the staging table
            cur.execute(
                f"""
                DELETE FROM "{self.schema}"."{stg_table}" a
                    USING (
                        SELECT ctid
                        FROM (
                            SELECT
                                ctid,
                                ROW_NUMBER() OVER (
                                    PARTITION BY event_id, program_date_time
                                    ORDER BY program_date_time DESC NULLS LAST
                                ) AS rn
                            FROM "{self.schema}"."{stg_table}"
                        ) t
                        WHERE rn > 1
                    ) b
                    WHERE a.ctid = b.ctid
                """
            )

            # Delete all matching rows in the prod table
            cur.execute(
                f"""
                DELETE FROM {self.schema}.{prod_table}
                WHERE (event_id, program_date_time) IN ( 
                    SELECT 
                        event_id, 
                        program_date_time
                    FROM {self.schema}.{stg_table}
                )
                """
            )

            print(f"Deleted {cur.rowcount} matching rows in prod table as well.")

    def clean_stg_records_and_insert_prod(
        self,
        watermark: date,
        source_name: str,
        stg_table: str,
        prod_table: str,
    ):
        print(f"[CLEAN STG → PROD] Starting for {source_name}")
        print(f"[CLEAN STG → PROD] STG table: {stg_table}, PROD table: {prod_table}")

        base_source_name = source_name.split("__", 1)[0]

        if base_source_name == EltWatermarks.RESERVATIONS:
            timestamp_col_name = "reservation_updated_at"
            print(f"[CLEAN STG → PROD] Running deduplication for reservations...")
            self.dedupe_reservation_records(stg_table, prod_table)
        elif base_source_name == EltWatermarks.RESERVATION_CANCELLATIONS:
            print(f"[CLEAN STG → PROD] Running deduplication for cancellations...")
            self.dedupe_reservation_cancellation_records(stg_table, prod_table)
            timestamp_col_name = "cancelled_on"
        # -------------------------------------------------------------------

        # grab the cleaned reservations
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{self.schema}"."{stg_table}"')
            stg_count = cur.fetchone()[0]
            print(f"[CLEAN STG → PROD] Records in STG after dedupe: {stg_count}")

            if stg_count > 0:
                cur.execute(f'SELECT * FROM "{self.schema}"."{stg_table}"')
                cols = [desc[0] for desc in cur.description]
                records = [dict(zip(cols, row)) for row in cur.fetchall()]
                print(
                    f"[CLEAN STG → PROD] Loaded {len(records)} records from STG for insertion into PROD"
                )
            else:
                records = []
                print(f"[CLEAN STG → PROD] No records to insert into PROD")

        # Check PROD count before insert
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{self.schema}"."{prod_table}"')
            prod_count_before = cur.fetchone()[0]
            print(
                f"[CLEAN STG → PROD] PROD table currently has {prod_count_before} records"
            )

        # insert into production
        if records:
            print(f"[CLEAN STG → PROD] Inserting {len(records)} records into PROD...")
            self.insert_records_into_prod_db(prod_table, records)

            # Check PROD count after insert
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) FROM "{self.schema}"."{prod_table}"')
                prod_count_after = cur.fetchone()[0]
                print(
                    f"[CLEAN STG → PROD] PROD table now has {prod_count_after} records "
                    f"(was {prod_count_before}, change: {prod_count_after - prod_count_before})"
                )
        else:
            print(f"[CLEAN STG → PROD] No records to insert, skipping PROD insert")

        # update watermark using last_loaded_at only
        print(f"[CLEAN STG → PROD] Updating watermark for {source_name}")
        self.update_elt_watermark(source_name)

        print(f"[CLEAN STG → PROD] Truncating STG table: {stg_table}")
        self.truncate_table(stg_table)

        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{self.schema}"."{stg_table}"')
            stg_count_final = cur.fetchone()[0]
            print(
                f"[CLEAN STG → PROD] STG table after truncate: {stg_count_final} records"
            )

        print(f"[CLEAN STG → PROD] Complete for {source_name}")


class InsertMixin:
    def insert_members(self, members: list[dict], table_name: str):
        total_members = len(members)
        print(
            f"[INSERT MEMBERS] Starting insert of {total_members} members into {table_name}"
        )

        rows = [
            (
                m["client_code"],
                m["member_id"],
                m["first_name"],
                m["last_name"],
                m["gender"],
                m["phone_number"],
                m["date_of_birth"],
                m.get("club_member_key"),
                m.get("email"),
                datetime.now(timezone.utc),
            )
            for m in members
        ]

        BATCH_SIZE = 1000
        total_batches = (len(rows) + BATCH_SIZE - 1) // BATCH_SIZE
        with self._connect() as conn, conn.cursor() as cur:
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i : i + BATCH_SIZE]
                batch_num = i // BATCH_SIZE + 1
                print(
                    f"[INSERT MEMBERS] Inserting batch {batch_num}/{total_batches} "
                    f"({len(batch)} records) into {table_name}"
                )
                execute_values(
                    cur,
                    f"""
                    INSERT INTO "{self.schema}"."{table_name}" (
                        client_code,
                        member_id,
                        first_name,
                        last_name,
                        gender,
                        phone_number,
                        date_of_birth,
                        club_member_key,
                        email,
                        created_at
                    ) VALUES %s
                    ON CONFLICT (client_code, member_id) DO UPDATE SET
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        gender = EXCLUDED.gender,
                        phone_number = EXCLUDED.phone_number,
                        date_of_birth = EXCLUDED.date_of_birth,
                        club_member_key = EXCLUDED.club_member_key,
                        email = EXCLUDED.email,
                        created_at = EXCLUDED.created_at
                    """,
                    batch,
                )
                print(
                    f"[INSERT MEMBERS] Batch {batch_num} complete: "
                    f"{cur.rowcount} rows affected (inserts + updates)"
                )

        print(
            f"[INSERT MEMBERS] Completed insert of {total_members} members into {table_name}"
        )

    def insert_records_into_prod_db(self, prod_table: str, records):
        if prod_table == Tables.RESERVATIONS_RAW:
            self.insert_reservations(records, prod_table)
        elif prod_table == Tables.RESERVATION_CANCELLATIONS_RAW:
            self.insert_reservation_cancellations(records, prod_table)

    def insert_reservation_cancellations(
        self, cancellations: list[dict], table_name: str
    ):
        rows = [
            (
                m["client_code"],
                m.get("source_system"),
                m["event_id"],
                m["reservation_id"],
                m["reservation_type"],
                m["reservation_created_at"],
                m["reservation_start_at"],
                m["reservation_end_at"],
                m["cancelled_on"],
                m["day_of_week"],
                m["is_program"],
                m["program_name"],
                m["player_name"],
                m["player_first_name"],
                m["player_last_name"],
                m["player_email"],
                m["player_phone"],
                m["fee"],
                m["is_team_event"],
                m["event_category_name"],
                m["event_category_id"],
                m["member_id"],
                datetime.now(timezone.utc),
            )
            for m in cancellations
        ]

        BATCH_SIZE = 1000
        with self._connect() as conn, conn.cursor() as cur:
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i : i + BATCH_SIZE]
                execute_values(
                    cur,
                    f"""
                    INSERT INTO "{self.schema}"."{table_name}" (
                        client_code,
                        source_system,
                        event_id,
                        reservation_id,
                        reservation_type,
                        reservation_created_at,
                        reservation_start_at,
                        reservation_end_at,
                        cancelled_on,
                        day_of_week,
                        is_program,
                        program_name,
                        player_name,
                        player_first_name,
                        player_last_name,
                        player_email,
                        player_phone,
                        fee,
                        is_team_event,
                        event_category_name,
                        event_category_id,
                        member_id,
                        created_at
                    ) VALUES %s
                    ON CONFLICT DO NOTHING
                    """,
                    batch,
                )
                print(f"Inserted batch {i // BATCH_SIZE + 1}")

    def insert_event_summaries(self, events: list[dict], table_name: str):
        print(f"About to insert {len(events)} rows into {table_name}")

        rows = [
            (
                m["event_id"],
                m["program_name"],
                m["event_category_id"],
                m["program_date_time"],
                m["event_category_name"],
                m["instructors"],
                m["percent_filled"],
                m["num_registered"],
                m["max_registrants"],
                m["total_revenue"],
                m["revenue_per_player"],
                m["daily_price_member"],
                m["daily_price_non_member"],
                m["entire_event_price_member"],
                m["entire_event_price_non_member"],
                m["program_start_timestamp"],
                m["program_end_timestamp"],
                m["event_skill_level"],
                m["event_type"],
                (
                    json.dumps(m.get("rating_restrictions"))
                    if not isinstance(m.get("rating_restrictions"), str)
                    else m.get("rating_restrictions")
                ),
                m["event_tags"],
                m["is_series"],
            )
            for m in events
        ]

        BATCH_SIZE = 1000
        with self._connect() as conn, conn.cursor() as cur:
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i : i + BATCH_SIZE]
                execute_values(
                    cur,
                    f"""
                    INSERT INTO "{self.schema}"."{table_name}" (
                        event_id,
                        program_name,
                        event_category_id,
                        program_date_time,
                        event_category_name,
                        instructors,
                        percent_filled,
                        num_registered,
                        max_registrants,
                        total_revenue,
                        revenue_per_player,
                        daily_price_member,
                        daily_price_non_member,
                        entire_event_price_member,
                        entire_event_price_non_member,
                        program_start_timestamp,
                        program_end_timestamp,
                        event_skill_level,
                        event_type,
                        rating_restrictions,
                        event_tags,
                        is_series
                    ) VALUES %s
                    """,
                    batch,
                )
                print(f"Inserted batch {i // BATCH_SIZE + 1}")

    def insert_reservations(self, reservations: list[dict], table_name: str) -> None:
        total_reservations = len(reservations)
        print(
            f"[INSERT RESERVATIONS] Starting insert of {total_reservations} reservations into {table_name}"
        )

        # Deduplicate reservations by (client_code, reservation_id, member_id)
        # Keep the latest version based on reservation_updated_at or last occurrence
        seen = {}
        for m in reservations:
            key = (m["client_code"], m["reservation_id"], m["member_id"])
            existing = seen.get(key)
            if existing is None:
                seen[key] = m
            else:
                # Keep the one with the latest updated_at, or last one if both are None
                existing_updated = existing.get("reservation_updated_at")
                current_updated = m.get("reservation_updated_at")
                if existing_updated is None or (current_updated is not None and current_updated > existing_updated):
                    seen[key] = m
        
        deduplicated_reservations = list(seen.values())
        if len(deduplicated_reservations) < total_reservations:
            duplicates_removed = total_reservations - len(deduplicated_reservations)
            print(
                f"[INSERT RESERVATIONS] Removed {duplicates_removed} duplicate reservations "
                f"(keeping most recent by reservation_updated_at)"
            )

        rows = [
            (
                m["client_code"],
                m["event_id"],
                m["reservation_id"],
                m["reservation_created_at"],
                m["reservation_updated_at"],
                m["reservation_start_at"],
                m["reservation_end_at"],
                m.get("reservation_cancelled_at"),
                m["member_id"],
                datetime.now(timezone.utc),
            )
            for m in deduplicated_reservations
        ]

        BATCH_SIZE = 1000
        total_batches = (len(rows) + BATCH_SIZE - 1) // BATCH_SIZE
        is_prod_table = table_name == Tables.RESERVATIONS_RAW
        with self._connect() as conn, conn.cursor() as cur:
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i : i + BATCH_SIZE]
                batch_num = i // BATCH_SIZE + 1
                print(
                    f"[INSERT RESERVATIONS] Inserting batch {batch_num}/{total_batches} "
                    f"({len(batch)} records) into {table_name}"
                )
                if is_prod_table:
                    execute_values(
                        cur,
                        f"""
                        INSERT INTO "{self.schema}"."{table_name}" (
                            client_code,
                            event_id,
                            reservation_id,
                            reservation_created_at,
                            reservation_updated_at,
                            reservation_start_at,
                            reservation_end_at,
                            reservation_cancelled_at,
                            member_id,
                            created_at
                        ) VALUES %s
                        ON CONFLICT (client_code, reservation_id, member_id) DO UPDATE SET
                            event_id = EXCLUDED.event_id,
                            reservation_created_at = EXCLUDED.reservation_created_at,
                            reservation_updated_at = EXCLUDED.reservation_updated_at,
                            reservation_start_at = EXCLUDED.reservation_start_at,
                            reservation_end_at = EXCLUDED.reservation_end_at,
                            reservation_cancelled_at = EXCLUDED.reservation_cancelled_at,
                            created_at = EXCLUDED.created_at
                        """,
                        batch,
                    )
                else:
                    # Staging table: handle conflicts on (client_code, reservation_id, member_id)
                    # This allows multiple people with the same reservation_id
                    # Update with latest data to ensure we have the most recent information
                    execute_values(
                        cur,
                        f"""
                        INSERT INTO "{self.schema}"."{table_name}" (
                            client_code,
                            event_id,
                            reservation_id,
                            reservation_created_at,
                            reservation_updated_at,
                            reservation_start_at,
                            reservation_end_at,
                            reservation_cancelled_at,
                            member_id,
                            created_at
                        ) VALUES %s
                        ON CONFLICT (client_code, reservation_id, member_id) DO UPDATE SET
                            event_id = EXCLUDED.event_id,
                            reservation_created_at = EXCLUDED.reservation_created_at,
                            reservation_updated_at = EXCLUDED.reservation_updated_at,
                            reservation_start_at = EXCLUDED.reservation_start_at,
                            reservation_end_at = EXCLUDED.reservation_end_at,
                            reservation_cancelled_at = EXCLUDED.reservation_cancelled_at,
                            created_at = EXCLUDED.created_at
                        """,
                        batch,
                    )
                print(
                    f"[INSERT RESERVATIONS] Batch {batch_num} complete: "
                    f"{cur.rowcount} rows affected"
                )

        print(
            f"[INSERT RESERVATIONS] Completed insert of {len(deduplicated_reservations)} reservations into {table_name}"
        )

    def insert_events(
        self, events: list[dict], table_name: str = "facility_events_raw"
    ) -> None:
        """
        Replace all events for each client_code + source_system combination.
        This ensures deleted events are removed from the database.
        """
        if not events:
            print("[REPLACE EVENTS] No events to insert")
            return

        # Deduplicate events by (client_code, source_system, event_id, event_start_time)
        # Keep the last occurrence of each duplicate
        seen = {}
        for event in events:
            # event_start_time is required for the primary key
            if not event.get("event_start_time"):
                print(
                    f"[REPLACE EVENTS] WARNING: Skipping event {event.get('event_id')} "
                    f"from {event.get('client_code')} - missing event_start_time"
                )
                continue
            key = (
                event["client_code"],
                event["source_system"],
                event["event_id"],
                event["event_start_time"],
            )
            seen[key] = event  # Always keep the latest occurrence

        deduplicated_events = list(seen.values())

        if len(deduplicated_events) < len(events):
            print(
                f"[REPLACE EVENTS] Deduplicated {len(events)} events to {len(deduplicated_events)} "
                f"(removed {len(events) - len(deduplicated_events)} duplicates)"
            )

        # Group events by (client_code, source_system) to replace each combination separately
        events_by_client = {}
        for event in deduplicated_events:
            key = (event["client_code"], event["source_system"])
            if key not in events_by_client:
                events_by_client[key] = []
            events_by_client[key].append(event)

        for (client_code, source_system), client_events in events_by_client.items():
            print(
                f"[REPLACE EVENTS] Replacing events for {client_code}/{source_system}: "
                f"{len(client_events)} events"
            )

            rows = [
                (
                    m["client_code"],
                    m["source_system"],
                    m["event_id"],
                    m.get("event_name"),
                    m.get("event_type"),
                    m.get("event_start_time"),
                    m.get("event_end_time"),
                    m.get("num_registrants"),
                    m.get("max_registrants"),
                    m.get("admission_rate_regular"),
                    m.get("admission_rate_member"),
                    datetime.now(timezone.utc),
                )
                for m in client_events
            ]

            staging_table = f"{table_name}_stg"
            prod_table = table_name

            with self._connect() as conn, conn.cursor() as cur:
                # Step 1: Create staging table if it doesn't exist
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS "{self.schema}"."{staging_table}" (
                        LIKE "{self.schema}"."{prod_table}" INCLUDING DEFAULTS INCLUDING CONSTRAINTS
                    )
                """
                )

                # Step 2: Truncate staging table
                cur.execute(f'TRUNCATE TABLE "{self.schema}"."{staging_table}"')

                # Step 3: Insert new data into staging table
                BATCH_SIZE = 1000
                for i in range(0, len(rows), BATCH_SIZE):
                    batch = rows[i : i + BATCH_SIZE]
                    batch_num = i // BATCH_SIZE + 1
                    execute_values(
                        cur,
                        f"""
                        INSERT INTO "{self.schema}"."{staging_table}" (
                            client_code,
                            source_system,
                            event_id,
                            event_name,
                            event_type,
                            event_start_time,
                            event_end_time,
                            num_registrants,
                            max_registrants,
                            admission_rate_regular,
                            admission_rate_member,
                            created_at
                        ) VALUES %s
                        """,
                        batch,
                    )

                # Step 4: Get row counts before swap
                cur.execute(
                    f"""
                    SELECT COUNT(*) FROM "{self.schema}"."{prod_table}"
                    WHERE client_code = %s AND source_system = %s
                    """,
                    (client_code, source_system),
                )
                old_count = cur.fetchone()[0]
                cur.execute(f'SELECT COUNT(*) FROM "{self.schema}"."{staging_table}"')
                new_count = cur.fetchone()[0]

                # Step 5: Commit staging inserts
                conn.commit()

                # Step 6: Replace data atomically in a transaction
                cur.execute("BEGIN")
                try:
                    # Delete existing records for this client_code + source_system
                    cur.execute(
                        f"""
                        DELETE FROM "{self.schema}"."{prod_table}"
                        WHERE client_code = %s AND source_system = %s
                        """,
                        (client_code, source_system),
                    )

                    # Copy all data from staging to production
                    cur.execute(
                        f"""
                        INSERT INTO "{self.schema}"."{prod_table}" (
                            client_code,
                            source_system,
                            event_id,
                            event_name,
                            event_type,
                            event_start_time,
                            event_end_time,
                            num_registrants,
                            max_registrants,
                            admission_rate_regular,
                            admission_rate_member,
                            created_at
                        )
                        SELECT 
                            client_code,
                            source_system,
                            event_id,
                            event_name,
                            event_type,
                            event_start_time,
                            event_end_time,
                            num_registrants,
                            max_registrants,
                            admission_rate_regular,
                            admission_rate_member,
                            created_at
                        FROM "{self.schema}"."{staging_table}"
                    """
                    )

                    cur.execute("COMMIT")

                    print(
                        f"[REPLACE EVENTS] ✓ Complete for {client_code}/{source_system}: "
                        f"Replaced {old_count} rows with {new_count} rows"
                    )
                except Exception as e:
                    cur.execute("ROLLBACK")
                    print(
                        f"[REPLACE EVENTS] Error during replacement for {client_code}/{source_system}: {e}"
                    )
                    raise

        print(
            f"[REPLACE EVENTS] Completed replacement for {len(events_by_client)} client/system combinations"
        )

    def replace_court_availabilities(
        self,
        availabilities: list[dict],
        table_name: str = "facility_court_availabilities",
    ) -> None:
        """
        Replace all court availabilities for each client_code + source_system combination.
        This ensures deleted availabilities are removed from the database.
        """
        if not availabilities:
            print("[REPLACE COURT AVAILABILITIES] No availabilities to insert")
            return

        # Group availabilities by (client_code, source_system) to replace each combination separately
        availabilities_by_client = {}
        for avail in availabilities:
            key = (avail["client_code"], avail["source_system"])
            if key not in availabilities_by_client:
                availabilities_by_client[key] = []
            availabilities_by_client[key].append(avail)

        staging_table = f"{table_name}_stg"
        prod_table = table_name

        for (client_code, source_system), client_availabilities in availabilities_by_client.items():
            print(
                f"[REPLACE COURT AVAILABILITIES] Replacing availabilities for {client_code}/{source_system}: "
                f"{len(client_availabilities)} availabilities"
            )

            rows = [
                (
                    m["client_code"],
                    m["source_system"],
                    m["court_id"],
                    m.get("court_name"),
                    m["slot_start"],
                    m["slot_end"],
                    m.get("period_type"),
                    datetime.now(timezone.utc),
                )
                for m in client_availabilities
            ]

            with self._connect() as conn, conn.cursor() as cur:
                # Step 1: Create staging table if it doesn't exist
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS "{self.schema}"."{staging_table}" (
                        LIKE "{self.schema}"."{prod_table}" INCLUDING DEFAULTS INCLUDING CONSTRAINTS
                    )
                """
                )

                # Step 2: Truncate staging table
                cur.execute(f'TRUNCATE TABLE "{self.schema}"."{staging_table}"')

                # Step 3: Insert new data into staging table
                BATCH_SIZE = 1000
                total_batches = (len(rows) + BATCH_SIZE - 1) // BATCH_SIZE
                for i in range(0, len(rows), BATCH_SIZE):
                    batch = rows[i : i + BATCH_SIZE]
                    batch_num = i // BATCH_SIZE + 1
                    print(
                        f"[REPLACE COURT AVAILABILITIES] Inserting batch {batch_num}/{total_batches} "
                        f"({len(batch)} records) into staging for {client_code}/{source_system}"
                    )
                    execute_values(
                        cur,
                        f"""
                        INSERT INTO "{self.schema}"."{staging_table}" (
                            client_code,
                            source_system,
                            court_id,
                            court_name,
                            slot_start,
                            slot_end,
                            period_type,
                            created_at
                        ) VALUES %s
                        """,
                        batch,
                    )

                # Step 4: Get row counts before swap
                cur.execute(
                    f"""
                    SELECT COUNT(*) FROM "{self.schema}"."{prod_table}"
                    WHERE client_code = %s AND source_system = %s
                    """,
                    (client_code, source_system),
                )
                old_count = cur.fetchone()[0]
                cur.execute(f'SELECT COUNT(*) FROM "{self.schema}"."{staging_table}"')
                new_count = cur.fetchone()[0]

                # Step 5: Commit staging inserts
                conn.commit()

                # Step 6: Replace data atomically in a transaction
                cur.execute("BEGIN")
                try:
                    # Delete existing records for this client_code + source_system
                    cur.execute(
                        f"""
                        DELETE FROM "{self.schema}"."{prod_table}"
                        WHERE client_code = %s AND source_system = %s
                        """,
                        (client_code, source_system),
                    )

                    # Copy all data from staging to production
                    cur.execute(
                        f"""
                        INSERT INTO "{self.schema}"."{prod_table}" (
                            client_code,
                            source_system,
                            court_id,
                            court_name,
                            slot_start,
                            slot_end,
                            period_type,
                            created_at
                        )
                        SELECT 
                            client_code,
                            source_system,
                            court_id,
                            court_name,
                            slot_start,
                            slot_end,
                            period_type,
                            created_at
                        FROM "{self.schema}"."{staging_table}"
                    """
                    )

                    cur.execute("COMMIT")

                    print(
                        f"[REPLACE COURT AVAILABILITIES] ✓ Complete for {client_code}/{source_system}: "
                        f"Replaced {old_count} rows with {new_count} rows"
                    )
                except Exception as e:
                    cur.execute("ROLLBACK")
                    print(
                        f"[REPLACE COURT AVAILABILITIES] Error during replacement for {client_code}/{source_system}: {e}"
                    )
                    raise

        print(
            f"[REPLACE COURT AVAILABILITIES] Completed replacement for {len(availabilities_by_client)} client/system combinations"
        )

    def insert_transactions(self, transactions: list[dict], table_name: str) -> None:
        rows = [
            (
                m["transaction_id"],
                m["transaction_type"],
                m["transaction_date"],
                m["subtotal"],
                m["tax_total"],
                m["total"],
                m["unpaid_amount"],
                m["paid_on"],
                m["payment_type"],
                m["category"],
                m["reservation_start"],
                m["reservation_end"],
                m["instructors"],
                m["organization_member_id"],
                m["organization_first_name"],
                m["organization_last_name"],
                m["organization_member_email"],
                m["organization_member_phone"],
                m["account_creation_date"],
            )
            for m in transactions
        ]

        total_incoming = len(rows)
        total_inserted = 0
        BATCH_SIZE = 1000

        with self._connect() as conn, conn.cursor() as cur:
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i : i + BATCH_SIZE]
                execute_values(
                    cur,
                    f"""
                    INSERT INTO "{self.schema}"."{table_name}" (
                        transaction_id,
                        transaction_type,
                        transaction_date,
                        subtotal,
                        tax_total,
                        total,
                        unpaid_amount,
                        paid_on,
                        payment_type,
                        category,
                        reservation_start,
                        reservation_end,
                        instructors,
                        organization_member_id,
                        organization_first_name,
                        organization_last_name,
                        organization_member_email,
                        organization_member_phone,
                        account_creation_date
                    ) VALUES %s
                    ON CONFLICT (transaction_id) DO NOTHING
                    """,
                    batch,
                )
                print(f"Inserted batch {i // BATCH_SIZE + 1}")
                total_inserted += cur.rowcount
        print(
            f"Transactions received: {total_incoming}, inserted: {total_inserted}, skipped: {total_incoming - total_inserted}"
        )
