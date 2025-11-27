-- Staging layer for courts table
select
    id as court_id,
    client_code,
    label as court_label,
    type_name as court_type,
    order_index,
    created_at,
    updated_at
from {{ source('raw', 'courts') }}

