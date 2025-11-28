select
    o.client_code,
    o.facility_display_name as client_name,
    o.location_display_name,
    null as timezone,  -- TODO: Add timezone to organizations if needed
    o.is_customer as is_active,
    null as notes,  -- TODO: Add notes to organizations if needed
    o.source_system_code as reservation_system_code,
    case 
        when o.source_system_code = 'courtreserve' then 'CourtReserve'
        when o.source_system_code = 'podplay' then 'Podplay'
        else o.source_system_code
    end as reservation_system_name,
    null as api_base_url,  -- Not stored in organizations
    null as auth_method,  -- Not stored in organizations
    null as reservation_system_notes  -- Not stored in organizations
from {{ ref('stg_organizations') }} o

