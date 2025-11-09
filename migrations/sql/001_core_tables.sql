-- 001_core_tables.sql
-- Core ingestion tables required for Courtlink ELT pipelines.

-- ELT watermarks ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{SCHEMA}}.elt_watermarks (
    source_name            TEXT PRIMARY KEY,
    last_loaded_at         TIMESTAMPTZ,
    last_record_created_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS elt_watermarks_source_name_idx
    ON {{SCHEMA}}.elt_watermarks (source_name);

-- Members -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{SCHEMA}}.members_raw (
    client_code   TEXT NOT NULL,
    member_id     TEXT NOT NULL,
    first_name    TEXT,
    last_name     TEXT,
    gender        TEXT,
    date_of_birth DATE,
    phone_number  TEXT,
    email         TEXT,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS members_raw_client_member_idx
    ON {{SCHEMA}}.members_raw (client_code, member_id);

CREATE TABLE IF NOT EXISTS {{SCHEMA}}.members_raw_stg (
    client_code      TEXT NOT NULL,
    member_id        TEXT NOT NULL,
    first_name       TEXT,
    last_name        TEXT,
    gender           TEXT,
    date_of_birth    DATE,
    phone_number     TEXT,
    club_member_key  TEXT,
    email            TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS members_raw_stg_client_member_idx
    ON {{SCHEMA}}.members_raw_stg (client_code, member_id);

-- Reservations --------------------------------------------------------------
CREATE TABLE IF NOT EXISTS {{SCHEMA}}.reservations_raw (
    client_code             TEXT NOT NULL,
    reservation_id          TEXT NOT NULL,
    event_id                TEXT,
    member_id               TEXT,
    reservation_created_at  TIMESTAMPTZ,
    reservation_updated_at  TIMESTAMPTZ,
    reservation_start_at    TIMESTAMPTZ,
    reservation_end_at      TIMESTAMPTZ,
    created_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS reservations_raw_client_res_member_idx
    ON {{SCHEMA}}.reservations_raw (client_code, reservation_id, member_id);

CREATE TABLE IF NOT EXISTS {{SCHEMA}}.reservations_raw_stg (
    client_code             TEXT NOT NULL,
    reservation_id          TEXT NOT NULL,
    event_id                TEXT,
    member_id               TEXT,
    reservation_created_at  TIMESTAMPTZ,
    reservation_updated_at  TIMESTAMPTZ,
    reservation_start_at    TIMESTAMPTZ,
    reservation_end_at      TIMESTAMPTZ,
    created_at              TIMESTAMPTZ DEFAULT NOW()
);

-- Reservation cancellations -------------------------------------------------
CREATE TABLE IF NOT EXISTS {{SCHEMA}}.reservation_cancellations_raw (
    event_id               TEXT,
    reservation_id         TEXT,
    reservation_type       TEXT,
    reservation_created_at TIMESTAMPTZ,
    reservation_start_at   TIMESTAMPTZ,
    reservation_end_at     TIMESTAMPTZ,
    cancelled_on           TIMESTAMPTZ,
    day_of_week            TEXT,
    is_program             BOOLEAN,
    program_name           TEXT,
    player_name            TEXT,
    player_first_name      TEXT,
    player_last_name       TEXT,
    player_email           TEXT,
    player_phone           TEXT,
    fee                    NUMERIC,
    is_team_event          BOOLEAN,
    event_category_name    TEXT,
    event_category_id      TEXT,
    member_id              TEXT,
    created_at             TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {{SCHEMA}}.reservation_cancellations_raw_stg (
    event_id               TEXT,
    reservation_id         TEXT,
    reservation_type       TEXT,
    reservation_created_at TIMESTAMPTZ,
    reservation_start_at   TIMESTAMPTZ,
    reservation_end_at     TIMESTAMPTZ,
    cancelled_on           TIMESTAMPTZ,
    day_of_week            TEXT,
    is_program             BOOLEAN,
    program_name           TEXT,
    player_name            TEXT,
    player_first_name      TEXT,
    player_last_name       TEXT,
    player_email           TEXT,
    player_phone           TEXT,
    fee                    NUMERIC,
    is_team_event          BOOLEAN,
    event_category_name    TEXT,
    event_category_id      TEXT,
    member_id              TEXT,
    created_at             TIMESTAMPTZ DEFAULT NOW()
);

