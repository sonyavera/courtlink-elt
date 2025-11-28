-- Staging layer for organizations table
select
    id as organization_id,
    source_system_code,
    client_code,
    login_link,
    city,
    is_customer,
    hourly_rate_non_member,
    hourly_rate_member,
    hourly_rate_non_member_off_peak,
    hourly_rate_member_off_peak,
    facility_display_name,
    peak_hours,
    location_display_name,
    podplay_pod,
    podplay_pod_id,
    operating_hours
from {{ source('raw', 'organizations') }}

