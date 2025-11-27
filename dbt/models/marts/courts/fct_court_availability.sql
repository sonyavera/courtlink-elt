{{ config(materialized='table', tags=['courts']) }}

with raw_availability as (
    select
        id,
        client_code,
        source_system,
        court_id,
        court_name,
        slot_start,
        slot_end,
        period_type,
        created_at
    from {{ source('raw', 'facility_court_availabilities') }}
),

courts as (
    select
        court_id,
        client_code,
        court_label,
        court_type,
        order_index
    from {{ ref('stg_courts') }}
),

organizations as (
    select
        client_code,
        peak_hours,
        operating_hours
    from {{ ref('stg_organizations') }}
),

-- Determine period_type based on peak_hours
-- Peak hours are stored in local timezone, but slot_start is in UTC
-- We need to convert slot_start to local time for comparison
availability_with_period as (
    select
        raw_availability.id,
        raw_availability.client_code,
        raw_availability.source_system,
        raw_availability.court_id,
        raw_availability.slot_start,
        raw_availability.slot_end,
        raw_availability.period_type,
        raw_availability.created_at,
        coalesce(courts.court_label, raw_availability.court_name) as court_name,
        organizations.peak_hours,
        -- Get timezone from peak_hours or operating_hours (default to America/New_York)
        coalesce(
            organizations.peak_hours->>'timezone',
            organizations.operating_hours->>'timezone',
            'America/New_York'
        ) as facility_timezone,
        -- Extract day of week (0=Sunday, 6=Saturday)
        extract(dow from raw_availability.slot_start) as day_of_week,
        -- Convert slot_start to local timezone and extract time
        (raw_availability.slot_start at time zone 'UTC' at time zone coalesce(
            organizations.peak_hours->>'timezone',
            organizations.operating_hours->>'timezone',
            'America/New_York'
        ))::time as slot_time_local,
        -- Determine if weekday (Monday-Friday = 1-5)
        case when extract(dow from raw_availability.slot_start) between 1 and 5 then true else false end as is_weekday
    from raw_availability
    left join courts
        on raw_availability.court_id::text = courts.court_id::text
        and raw_availability.client_code = courts.client_code
    left join organizations
        on raw_availability.client_code = organizations.client_code
),

-- Calculate period_type from peak_hours
-- Use lateral join to check if slot_time falls within any peak hour range
availability_with_calculated_period as (
    select
        a.*,
        case
            when a.peak_hours is null then null
            when a.is_weekday then
                -- Check weekday peak hours first
                case
                    when a.peak_hours->'weekday_peak' is not null 
                         and jsonb_typeof(a.peak_hours->'weekday_peak') = 'array'
                         and jsonb_array_length(a.peak_hours->'weekday_peak') > 0 then
                        case
                            when exists (
                                select 1
                                from jsonb_array_elements(a.peak_hours->'weekday_peak') as peak_range
                                where (
                                    -- Normal range (start <= end): time >= start AND time < end
                                    (peak_range->>'start')::time <= (peak_range->>'end')::time
                                    and a.slot_time_local >= (peak_range->>'start')::time 
                                    and a.slot_time_local < (peak_range->>'end')::time
                                ) or (
                                    -- Midnight crossing (start > end): time >= start OR time < end
                                    (peak_range->>'start')::time > (peak_range->>'end')::time
                                    and (
                                        a.slot_time_local >= (peak_range->>'start')::time 
                                        or a.slot_time_local < (peak_range->>'end')::time
                                    )
                                )
                            ) then 'peak'
                            -- Check if explicitly in weekday_off_peak
                            when a.peak_hours->'weekday_off_peak' is not null 
                                 and jsonb_typeof(a.peak_hours->'weekday_off_peak') = 'array'
                                 and jsonb_array_length(a.peak_hours->'weekday_off_peak') > 0
                                 and exists (
                                     select 1
                                     from jsonb_array_elements(a.peak_hours->'weekday_off_peak') as off_peak_range
                                     where (
                                         -- Normal range (start <= end): time >= start AND time < end
                                         (off_peak_range->>'start')::time <= (off_peak_range->>'end')::time
                                         and a.slot_time_local >= (off_peak_range->>'start')::time 
                                         and a.slot_time_local < (off_peak_range->>'end')::time
                                     ) or (
                                         -- Midnight crossing (start > end): time >= start OR time < end
                                         (off_peak_range->>'start')::time > (off_peak_range->>'end')::time
                                         and (
                                             a.slot_time_local >= (off_peak_range->>'start')::time 
                                             or a.slot_time_local < (off_peak_range->>'end')::time
                                         )
                                     )
                                 ) then 'off_peak'
                            -- If weekday_off_peak doesn't exist or is empty, everything else is peak
                            else 'peak'
                        end
                    else null
                end
            else
                -- Check weekend peak hours
                case
                    when a.peak_hours->'weekend_peak' is not null 
                         and jsonb_typeof(a.peak_hours->'weekend_peak') = 'array'
                         and jsonb_array_length(a.peak_hours->'weekend_peak') > 0 then
                        case
                            when exists (
                                select 1
                                from jsonb_array_elements(a.peak_hours->'weekend_peak') as peak_range
                                where (
                                    -- Normal range (start <= end): time >= start AND time < end
                                    (peak_range->>'start')::time <= (peak_range->>'end')::time
                                    and a.slot_time_local >= (peak_range->>'start')::time 
                                    and a.slot_time_local < (peak_range->>'end')::time
                                ) or (
                                    -- Midnight crossing (start > end): time >= start OR time < end
                                    (peak_range->>'start')::time > (peak_range->>'end')::time
                                    and (
                                        a.slot_time_local >= (peak_range->>'start')::time 
                                        or a.slot_time_local < (peak_range->>'end')::time
                                    )
                                )
                            ) then 'peak'
                            -- If weekend_off_peak doesn't exist or is empty, everything else is peak
                            else 'peak'
                        end
                    else null
                end
        end as calculated_period_type
    from availability_with_period a
)

select
    {{ dbt_utils.generate_surrogate_key(['id']) }} as availability_pk,
    client_code,
    source_system,
    court_id,
    slot_start,
    slot_end,
    extract(epoch from (slot_end - slot_start)) / 60 as duration_minutes,
    created_at,
    court_name,
    coalesce(calculated_period_type, period_type) as period_type
from availability_with_calculated_period
where slot_start >= current_date
    and slot_start <= current_date + interval '7 days'
order by client_code, slot_start, court_id

