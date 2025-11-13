with base as (
    select
        m.client_code,
        m.member_id,
        m.first_name,
        m.last_name,
        m.gender,
        m.date_of_birth,
        m.phone_number,
        m.email,
        m.created_at
    from {{ source('raw', 'members_raw') }} m
)

select
    md5(
        concat_ws(
            '||',
            coalesce(c.reservation_system_code, b.client_code),
            b.client_code,
            b.member_id
        )
    ) as member_pk,
    b.client_code,
    b.member_id as external_member_id,
    b.first_name,
    b.last_name,
    b.gender,
    b.date_of_birth as birthday,
    {{ normalize_phone('b.phone_number') }} as phone,
    {{ normalize_email('b.email') }} as email,
    b.created_at,
    coalesce(c.reservation_system_code, b.client_code) as reservation_system_code
from base b
left join {{ ref('clients') }} c
    on lower(b.client_code) = c.client_code

