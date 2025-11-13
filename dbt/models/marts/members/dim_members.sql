{{ config(materialized='table', tags=['members']) }}

select
    member_pk,
    client_code,
    external_member_id,
    first_name,
    last_name,
    gender,
    birthday,
    phone,
    email,
    created_at,
    reservation_system_code
from {{ ref('stg_members') }}

