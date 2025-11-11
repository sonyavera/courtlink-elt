-- Add client and source metadata to reservation cancellation tables
ALTER TABLE {{SCHEMA}}.reservation_cancellations_raw
    ADD COLUMN IF NOT EXISTS client_code TEXT,
    ADD COLUMN IF NOT EXISTS source_system TEXT;

ALTER TABLE {{SCHEMA}}.reservation_cancellations_raw_stg
    ADD COLUMN IF NOT EXISTS client_code TEXT,
    ADD COLUMN IF NOT EXISTS source_system TEXT;

-- Track cancellation timestamps directly on reservation tables
ALTER TABLE {{SCHEMA}}.reservations_raw
    ADD COLUMN IF NOT EXISTS reservation_cancelled_at TIMESTAMPTZ;

ALTER TABLE {{SCHEMA}}.reservations_raw_stg
    ADD COLUMN IF NOT EXISTS reservation_cancelled_at TIMESTAMPTZ;

-- Backfill cancellation metadata from existing composite identifiers
UPDATE {{SCHEMA}}.reservation_cancellations_raw
SET
    client_code = COALESCE(
        NULLIF(client_code, ''),
        NULLIF(split_part(event_id, ':', 1), ''),
        NULLIF(split_part(reservation_id, ':', 1), ''),
        NULLIF(split_part(member_id, ':', 1), '')
    ),
    source_system = COALESCE(source_system, 'courtreserve'),
    event_id = CASE
        WHEN event_id LIKE '%:%' THEN NULLIF(split_part(event_id, ':', 2), '')
        ELSE event_id
    END,
    reservation_id = CASE
        WHEN reservation_id LIKE '%:%' THEN NULLIF(split_part(reservation_id, ':', 2), '')
        ELSE reservation_id
    END,
    member_id = CASE
        WHEN member_id LIKE '%:%' THEN NULLIF(split_part(member_id, ':', 2), '')
        ELSE member_id
    END;

UPDATE {{SCHEMA}}.reservation_cancellations_raw_stg
SET
    client_code = COALESCE(
        NULLIF(client_code, ''),
        NULLIF(split_part(event_id, ':', 1), ''),
        NULLIF(split_part(reservation_id, ':', 1), ''),
        NULLIF(split_part(member_id, ':', 1), '')
    ),
    source_system = COALESCE(source_system, 'courtreserve'),
    event_id = CASE
        WHEN event_id LIKE '%:%' THEN NULLIF(split_part(event_id, ':', 2), '')
        ELSE event_id
    END,
    reservation_id = CASE
        WHEN reservation_id LIKE '%:%' THEN NULLIF(split_part(reservation_id, ':', 2), '')
        ELSE reservation_id
    END,
    member_id = CASE
        WHEN member_id LIKE '%:%' THEN NULLIF(split_part(member_id, ':', 2), '')
        ELSE member_id
    END;

