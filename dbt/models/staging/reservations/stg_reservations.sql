with unified as (
    select * from {{ ref('stg_reservations__courtreserve') }}
    union all
    select * from {{ ref('stg_reservations__podplay') }}
)

select
    u.reservation_pk,
    u.source_system_code,
    u.client_code,
    coalesce(o.source_system_code, u.source_system_code) as reservation_system_code,
    u.reservation_id,
    u.event_id,
    u.member_id,
    u.reservation_created_at,
    u.reservation_updated_at,
    u.reservation_start_at,
    u.reservation_end_at,
    u.ingested_at
from unified u
left join {{ ref('stg_organizations') }} o
    on u.client_code = o.client_code

