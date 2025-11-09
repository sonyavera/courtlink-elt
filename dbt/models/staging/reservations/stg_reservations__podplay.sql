with filtered as (
    select r.*
    from {{ source('raw', 'reservations_raw') }} r
    join {{ ref('clients') }} c
        on lower(r.client_code) = c.client_code
    where c.reservation_system_code = 'podplay'
)

select
    md5(
        concat_ws(
            '||',
            'podplay',
            client_code,
            coalesce(reservation_id, event_id),
            coalesce(reservation_start_at::text, '')
        )
    ) as reservation_pk,
    'podplay' as source_system_code,
    client_code,
    reservation_id,
    event_id,
    member_id,
    reservation_created_at,
    reservation_updated_at,
    reservation_start_at,
    reservation_end_at,
    created_at as ingested_at
from filtered

