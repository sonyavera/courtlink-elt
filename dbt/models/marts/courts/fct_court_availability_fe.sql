{{ config(materialized='table', tags=['courts', 'frontend']) }}

-- Frontend-optimized court availability model
-- Transforms 30-minute slots into:
-- 1. 1-hour slots starting at the top of the hour (e.g., 5:00-6:00, 6:00-7:00)
--    - Only created if BOTH 30-min halves are available (e.g., 5:00-5:30 AND 5:30-6:00)
-- 2. 30-minute "orphan" slots that don't form a complete hour
--    - If only 5:00-5:30 exists (no 5:30-6:00), show 5:00-5:30 as orphan
--    - If only 5:30-6:00 exists (no 5:00-5:30), show 5:30-6:00 as orphan
-- Includes readable duration strings and court counts

with base_availability as (
    select
        availability_pk,
        client_code,
        source_system,
        court_id,
        court_name,
        slot_start,
        slot_end,
        duration_minutes,
        period_type,
        created_at
    from {{ ref('fct_court_availability') }}
),

-- Identify all 30-minute slots, grouped by their position in the hour
-- Slots starting at :00 are "first half" (e.g., 5:00-5:30)
-- Slots starting at :30 are "second half" (e.g., 5:30-6:00)
slots_by_half as (
    select
        client_code,
        source_system,
        court_id,
        court_name,
        slot_start,
        slot_end,
        period_type,
        date_trunc('hour', slot_start) as hour_start,
        case
            when extract(minute from slot_start) = 0 then 'first_half'
            when extract(minute from slot_start) = 30 then 'second_half'
            else 'other'
        end as half_position
    from base_availability
    where duration_minutes = 30
),

-- Find courts that have BOTH halves available for the same hour and period_type
-- These will become 1-hour slots
full_hour_courts as (
    select
        f1.client_code,
        f1.source_system,
        f1.court_id,
        f1.hour_start,
        f1.period_type,
        f1.slot_start as first_half_start,
        f1.slot_end as first_half_end,
        f2.slot_start as second_half_start,
        f2.slot_end as second_half_end
    from slots_by_half f1
    inner join slots_by_half f2
        on f1.client_code = f2.client_code
        and f1.court_id = f2.court_id
        and f1.hour_start = f2.hour_start
        and f1.period_type = f2.period_type
        and f1.half_position = 'first_half'
        and f2.half_position = 'second_half'
        and f1.slot_end = f2.slot_start  -- Ensure they're consecutive
),

-- Create 1-hour slots by counting courts with full hour availability
-- Aggregate court names into comma-separated list
hour_availability as (
    select
        fhc.client_code,
        fhc.source_system,
        fhc.hour_start as slot_start,
        fhc.hour_start + interval '1 hour' as slot_end,
        count(distinct fhc.court_id) as available_courts_count,
        string_agg(distinct coalesce(s.court_name, 'Court ' || fhc.court_id), ', ' order by coalesce(s.court_name, 'Court ' || fhc.court_id)) as available_courts,
        fhc.period_type
    from full_hour_courts fhc
    left join slots_by_half s
        on fhc.client_code = s.client_code
        and fhc.court_id = s.court_id
        and fhc.hour_start = s.hour_start
        and s.half_position = 'first_half'
    group by fhc.client_code, fhc.source_system, fhc.hour_start, fhc.period_type
),

-- Find orphan slots - 30-minute slots that exist independently
-- For "double counting": include ALL courts that have a 30-min slot, even if they also form a full hour
-- First half orphans: ALL courts with 5:00-5:30 (regardless of whether they have 5:30-6:00)
first_half_orphans as (
    select
        f1.client_code,
        f1.source_system,
        f1.slot_start,
        f1.slot_end,
        f1.period_type,
        f1.court_id
    from slots_by_half f1
    where f1.half_position = 'first_half'
),

-- Second half orphans: ALL courts with 5:30-6:00 (for double counting)
-- This allows courts to appear in both the orphan slot and the hour slot if they have both halves
second_half_orphans as (
    select
        f2.client_code,
        f2.source_system,
        f2.slot_start,
        f2.slot_end,
        f2.period_type,
        f2.court_id
    from slots_by_half f2
    where f2.half_position = 'second_half'
),

-- Combine all orphan slots and count courts, aggregating court names
orphan_slots as (
    select
        combined_orphans.client_code,
        combined_orphans.source_system,
        combined_orphans.slot_start,
        combined_orphans.slot_end,
        count(distinct combined_orphans.court_id) as available_courts_count,
        string_agg(distinct coalesce(s.court_name, 'Court ' || combined_orphans.court_id), ', ' order by coalesce(s.court_name, 'Court ' || combined_orphans.court_id)) as available_courts,
        combined_orphans.period_type
    from (
        select client_code, source_system, slot_start, slot_end, period_type, court_id
        from first_half_orphans
        
        union all
        
        select client_code, source_system, slot_start, slot_end, period_type, court_id
        from second_half_orphans
    ) combined_orphans
    left join slots_by_half s
        on combined_orphans.client_code = s.client_code
        and combined_orphans.court_id = s.court_id
        and combined_orphans.slot_start = s.slot_start
        and combined_orphans.slot_end = s.slot_end
    group by combined_orphans.client_code, combined_orphans.source_system, combined_orphans.slot_start, combined_orphans.slot_end, combined_orphans.period_type
),

-- Combine hour slots and orphan slots
all_slots as (
    select
        client_code,
        source_system,
        slot_start,
        slot_end,
        available_courts_count,
        available_courts,
        period_type,
        'hour' as slot_type
    from hour_availability
    
    union all
    
    select
        client_code,
        source_system,
        slot_start,
        slot_end,
        available_courts_count,
        available_courts,
        period_type,
        'orphan' as slot_type
    from orphan_slots
),

-- Add duration in readable format
slots_with_duration as (
    select
        client_code,
        source_system,
        slot_start,
        slot_end,
        available_courts_count,
        available_courts,
        period_type,
        slot_type,
        extract(epoch from (slot_end - slot_start)) / 60 as duration_minutes,
        case
            when extract(epoch from (slot_end - slot_start)) / 60 = 30 then '30 minutes'
            when extract(epoch from (slot_end - slot_start)) / 60 = 60 then '1 hour'
            when extract(epoch from (slot_end - slot_start)) / 60 = 90 then '1.5 hours'
            when extract(epoch from (slot_end - slot_start)) / 60 = 120 then '2 hours'
            when extract(epoch from (slot_end - slot_start)) / 60 = 150 then '2.5 hours'
            when extract(epoch from (slot_end - slot_start)) / 60 = 180 then '3 hours'
            else concat(
                round(extract(epoch from (slot_end - slot_start)) / 60)::text,
                ' minutes'
            )
        end as duration_display
    from all_slots
)

select
    {{ dbt_utils.generate_surrogate_key(['client_code', 'slot_start', 'slot_end', 'slot_type']) }} as availability_pk,
    client_code,
    source_system,
    slot_start,
    slot_end,
    available_courts_count,
    available_courts,
    duration_minutes,
    duration_display,
    period_type,
    slot_type
from slots_with_duration
where slot_start >= current_date
    and slot_start <= current_date + interval '7 days'
order by client_code, slot_start, slot_type

