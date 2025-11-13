{{ config(materialized='table', tags=['members']) }}

with base as (
    select *
    from {{ ref('stg_reservations') }}
)

select
    reservation_pk,
    client_code,
    reservation_system_code,
    source_system_code,
    reservation_id,
    event_id,
    member_id,
    reservation_created_at,
    reservation_updated_at,
    reservation_start_at,
    reservation_end_at,
    ingested_at
from base

