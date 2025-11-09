select
    c.client_code,
    c.client_name,
    c.timezone,
    c.is_active,
    c.notes,
    c.reservation_system_code,
    rs.system_name as reservation_system_name,
    rs.api_base_url,
    rs.auth_method,
    rs.notes as reservation_system_notes
from {{ ref('clients') }} c
left join {{ ref('reservation_systems') }} rs
    on c.reservation_system_code = rs.system_code

