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
        m.membership_type_name,
        m.is_premium_member,
        m.member_since,
        m.created_at
    from {{ source('raw', 'members_raw') }} m
)

select
    md5(
        concat_ws(
            '||',
            coalesce(o.source_system_code, b.client_code),
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
    b.membership_type_name,
    b.is_premium_member,
    b.member_since,
    b.created_at,
    coalesce(o.source_system_code, b.client_code) as reservation_system_code
from base b
left join {{ ref('stg_organizations') }} o
    on lower(b.client_code) = o.client_code

