{{ config(materialized='table', tags=['courts', 'frontend']) }}

-- Frontend-optimized court availability model
-- Based directly on facility_court_availabilities (raw table)
-- Transforms 30-minute slots into:
-- 1. 1-hour slots starting at the top of the hour (e.g., 5:00-6:00, 6:00-7:00)
--    - Only created if BOTH 30-min halves are available (e.g., 5:00-5:30 AND 5:30-6:00)
-- 2. 2-hour slots starting at the top of the hour (e.g., 5:00-7:00, 6:00-8:00)
--    - Only created if BOTH consecutive 1-hour slots are available
--    - Court count is the minimum of the two 1-hour slots (you need a court for each hour)
-- 3. 30-minute "orphan" slots that don't form a complete hour
--    - If only 5:00-5:30 exists (no 5:30-6:00), show 5:00-5:30 as orphan
--    - If only 5:30-6:00 exists (no 5:00-5:30), show 5:30-6:00 as orphan
-- Includes readable duration strings and court counts

with base_availability as (
    select
        id,
        client_code,
        source_system,
        court_id,
        court_name,
        slot_start,
        slot_end,
        period_type,
        created_at,
        -- Calculate duration in minutes
        (extract(epoch from (slot_end - slot_start)) / 60)::integer as duration_minutes
    from {{ source('raw', 'facility_court_availabilities') }}
    where slot_start >= current_date
        and slot_start <= current_date + interval '7 days'
),

-- Identify all 30-minute slots, grouped by their position in the hour
-- Slots starting at :00 are "first half" (e.g., 5:00-5:30)
-- Slots starting at :30 are "second half" (e.g., 5:30-6:00)
-- Handle NULL period_type by treating it as matching (using COALESCE)
slots_by_half as (
    select
        client_code,
        source_system,
        court_id,
        court_name,
        slot_start,
        slot_end,
        coalesce(period_type, 'NULL') as period_type,  -- Normalize NULL to 'NULL' for matching
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
-- Use COALESCE to handle NULL period_type matching
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
        and f1.source_system = f2.source_system
        and f1.court_id = f2.court_id
        and f1.hour_start = f2.hour_start
        and f1.period_type = f2.period_type  -- Now handles NULL via COALESCE
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

-- Create 2-hour blocks by finding consecutive 1-hour slots
-- Court count is the minimum of the two 1-hour slots (you need a court for each hour)
-- Court names: if a court is available in BOTH hours, show full 2-hour range; otherwise show individual hour ranges
two_hour_courts_detail as (
    select
        h1.client_code,
        h1.source_system,
        h1.slot_start,
        h1.slot_end + interval '1 hour' as slot_end,
        h1.period_type,
        coalesce(s1.court_name, 'Court ' || fhc1.court_id) as court_name,
        fhc1.court_id,
        h1.slot_start as hour1_start,
        h1.slot_end as hour1_end,
        h2.slot_start as hour2_start,
        h2.slot_end as hour2_end,
        -- Check if this court is also available in hour 2
        exists (
            select 1
            from full_hour_courts fhc2
            where fhc2.client_code = h2.client_code
                and fhc2.hour_start = h2.slot_start
                and fhc2.period_type = h2.period_type
                and fhc2.court_id = fhc1.court_id
        ) as available_in_both_hours,
        'hour1' as which_hour
    from hour_availability h1
    inner join hour_availability h2
        on h1.client_code = h2.client_code
        and h1.source_system = h2.source_system
        and h1.period_type = h2.period_type
        and h1.slot_end = h2.slot_start  -- Consecutive hours
    inner join full_hour_courts fhc1
        on h1.client_code = fhc1.client_code
        and fhc1.hour_start = h1.slot_start
        and fhc1.period_type = h1.period_type
    left join slots_by_half s1
        on fhc1.client_code = s1.client_code
        and fhc1.court_id = s1.court_id
        and fhc1.hour_start = s1.hour_start
        and s1.half_position = 'first_half'
    
    union
    
    -- Courts only in hour 2 (not in hour 1)
    select
        h1.client_code,
        h1.source_system,
        h1.slot_start,
        h1.slot_end + interval '1 hour' as slot_end,
        h1.period_type,
        coalesce(s2.court_name, 'Court ' || fhc2.court_id) as court_name,
        fhc2.court_id,
        h1.slot_start as hour1_start,
        h1.slot_end as hour1_end,
        h2.slot_start as hour2_start,
        h2.slot_end as hour2_end,
        false as available_in_both_hours,
        'hour2' as which_hour
    from hour_availability h1
    inner join hour_availability h2
        on h1.client_code = h2.client_code
        and h1.source_system = h2.source_system
        and h1.period_type = h2.period_type
        and h1.slot_end = h2.slot_start
    inner join full_hour_courts fhc2
        on h2.client_code = fhc2.client_code
        and fhc2.hour_start = h2.slot_start
        and fhc2.period_type = h2.period_type
        -- Only include if NOT in hour 1
        and not exists (
            select 1
            from full_hour_courts fhc1
            where fhc1.client_code = h1.client_code
                and fhc1.hour_start = h1.slot_start
                and fhc1.period_type = h1.period_type
                and fhc1.court_id = fhc2.court_id
        )
    left join slots_by_half s2
        on fhc2.client_code = s2.client_code
        and fhc2.court_id = s2.court_id
        and fhc2.hour_start = s2.hour_start
        and s2.half_position = 'first_half'
),

two_hour_availability as (
    with court_formatted as (
        select
            t.client_code,
            t.source_system,
            t.slot_start,
            t.slot_end,
            t.period_type,
            t.court_name,
            t.court_id,
            case 
                when t.available_in_both_hours then
                    -- Court available in both hours: just show court name (no time range)
                    t.court_name
                else
                    -- Court only in one hour: show court name with time range for that hour
                    concat(
                        t.court_name,
                        ' (',
                        case when extract(minute from 
                            case when t.which_hour = 'hour1' then t.hour1_start else t.hour2_start end
                        ) = 0 
                            then trim(to_char(
                                case when t.which_hour = 'hour1' then t.hour1_start else t.hour2_start end,
                                'FMHH12 AM'
                            ))
                            else trim(to_char(
                                case when t.which_hour = 'hour1' then t.hour1_start else t.hour2_start end,
                                'FMHH12:MI AM'
                            ))
                        end,
                        '-',
                        case when extract(minute from 
                            case when t.which_hour = 'hour1' then t.hour1_end else t.hour2_end end
                        ) = 0 
                            then trim(to_char(
                                case when t.which_hour = 'hour1' then t.hour1_end else t.hour2_end end,
                                'FMHH12 AM'
                            ))
                            else trim(to_char(
                                case when t.which_hour = 'hour1' then t.hour1_end else t.hour2_end end,
                                'FMHH12:MI AM'
                            ))
                        end,
                        ')'
                    )
            end as court_display,
            t.available_in_both_hours,
            t.which_hour
        from two_hour_courts_detail t
    ),
    -- Deduplicate by court_id, prioritizing courts available in both hours
    court_deduplicated as (
        select distinct on (client_code, source_system, slot_start, period_type, court_id)
            client_code,
            source_system,
            slot_start,
            slot_end,
            period_type,
            court_display,
            available_in_both_hours,
            which_hour
        from court_formatted
        order by client_code, source_system, slot_start, period_type, court_id, available_in_both_hours desc
    ),
    -- Count partial courts by hour (courts only available in one hour) per 2-hour slot
    partial_court_count_by_hour as (
        select
            client_code,
            source_system,
            slot_start,
            period_type,
            which_hour,
            count(*) as partial_count
        from court_deduplicated
        where not available_in_both_hours
        group by client_code, source_system, slot_start, period_type, which_hour
    )
    select
        cd.client_code,
        cd.source_system,
        cd.slot_start,
        cd.slot_end,
        least(h1.available_courts_count, h2.available_courts_count) as available_courts_count,
        string_agg(
            cd.court_display,
            ', ' 
            order by cd.court_display
        ) as available_courts,
        cd.period_type
    from court_deduplicated cd
    inner join hour_availability h1
        on cd.client_code = h1.client_code
        and cd.slot_start = h1.slot_start
        and cd.period_type = h1.period_type
    inner join hour_availability h2
        on cd.client_code = h2.client_code
        and cd.slot_start + interval '1 hour' = h2.slot_start
        and cd.period_type = h2.period_type
    left join partial_court_count_by_hour pcc_hour1
        on cd.client_code = pcc_hour1.client_code
        and cd.source_system = pcc_hour1.source_system
        and cd.slot_start = pcc_hour1.slot_start
        and cd.period_type = pcc_hour1.period_type
        and pcc_hour1.which_hour = 'hour1'
    left join partial_court_count_by_hour pcc_hour2
        on cd.client_code = pcc_hour2.client_code
        and cd.source_system = pcc_hour2.source_system
        and cd.slot_start = pcc_hour2.slot_start
        and cd.period_type = pcc_hour2.period_type
        and pcc_hour2.which_hour = 'hour2'
    where 
        -- Include all courts available in both hours
        cd.available_in_both_hours = true
        -- OR include partial courts only if there are courts in the opposite hour to pair with
        or (
            cd.available_in_both_hours = false 
            and cd.which_hour is not null
            and (
                -- If court is in hour1, need hour2 partial courts to pair with
                (cd.which_hour = 'hour1' and coalesce(pcc_hour2.partial_count, 0) > 0)
                -- If court is in hour2, need hour1 partial courts to pair with
                or (cd.which_hour = 'hour2' and coalesce(pcc_hour1.partial_count, 0) > 0)
            )
        )
    group by cd.client_code, cd.source_system, cd.slot_start, cd.slot_end, cd.period_type, h1.available_courts_count, h2.available_courts_count
),

-- Find truly orphaned slots - 30-minute slots that DON'T have their matching half on the same court
-- These are candidates for combining across different courts
truly_orphaned_first_half as (
    select
        f1.client_code,
        f1.source_system,
        f1.slot_start,
        f1.slot_end,
        f1.period_type,
        f1.court_id,
        f1.court_name,
        f1.hour_start
    from slots_by_half f1
    where f1.half_position = 'first_half'
        -- Exclude if this court has the second half (it's not truly orphaned)
        and not exists (
            select 1
            from slots_by_half f2
            where f2.client_code = f1.client_code
                and f2.source_system = f1.source_system
                and f2.court_id = f1.court_id
                and f2.hour_start = f1.hour_start
                and f2.half_position = 'second_half'
                and f2.period_type = f1.period_type
        )
),

truly_orphaned_second_half as (
    select
        f2.client_code,
        f2.source_system,
        f2.slot_start,
        f2.slot_end,
        f2.period_type,
        f2.court_id,
        f2.court_name,
        f2.hour_start
    from slots_by_half f2
    where f2.half_position = 'second_half'
        -- Exclude if this court has the first half (it's not truly orphaned)
        and not exists (
            select 1
            from slots_by_half f1
            where f1.client_code = f2.client_code
                and f1.source_system = f2.source_system
                and f1.court_id = f2.court_id
                and f1.hour_start = f2.hour_start
                and f1.half_position = 'first_half'
                and f1.period_type = f2.period_type
        )
),

-- Combine truly orphaned slots across different courts to form 1-hour slots
-- Only combine if both are truly orphaned (can't form a full hour on their own)
cross_court_hour_slots as (
    select
        f1.client_code,
        f1.source_system,
        f1.hour_start as slot_start,
        f1.hour_start + interval '1 hour' as slot_end,
        f1.period_type,
        -- Format: "Court A (5-5:30 AM), Court B (5:30-6 AM)" or "Court A (3-4 PM), Court B (4-5 PM)"
        concat(
            coalesce(f1.court_name, 'Court ' || f1.court_id),
            ' (',
            case when extract(minute from f1.slot_start) = 0 
                then trim(to_char(f1.slot_start, 'FMHH12 AM'))
                else trim(to_char(f1.slot_start, 'FMHH12:MI AM'))
            end,
            '-',
            case when extract(minute from f1.slot_end) = 0 
                then trim(to_char(f1.slot_end, 'FMHH12 AM'))
                else trim(to_char(f1.slot_end, 'FMHH12:MI AM'))
            end,
            '), ',
            coalesce(f2.court_name, 'Court ' || f2.court_id),
            ' (',
            case when extract(minute from f2.slot_start) = 0 
                then trim(to_char(f2.slot_start, 'FMHH12 AM'))
                else trim(to_char(f2.slot_start, 'FMHH12:MI AM'))
            end,
            '-',
            case when extract(minute from f2.slot_end) = 0 
                then trim(to_char(f2.slot_end, 'FMHH12 AM'))
                else trim(to_char(f2.slot_end, 'FMHH12:MI AM'))
            end,
            ')'
        ) as available_courts,
        1 as available_courts_count  -- Always 1 combination
    from truly_orphaned_first_half f1
    inner join truly_orphaned_second_half f2
        on f1.client_code = f2.client_code
        and f1.source_system = f2.source_system
        and f1.period_type = f2.period_type
        and f1.hour_start = f2.hour_start
        and f1.court_id != f2.court_id  -- Must be different courts
        and f1.slot_end = f2.slot_start  -- Must be consecutive
),

-- Find orphan slots - 30-minute slots that exist independently
-- EXCLUDE slots that form a full hour on the same court (only show truly orphaned slots)
-- First half orphans: courts with 5:00-5:30 that DON'T have 5:30-6:00 on the same court
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
        -- Exclude if this court has the second half (it forms a full hour, so don't show as orphan)
        -- Check that slots are consecutive (first half ends exactly when second half starts)
        and not exists (
            select 1
            from slots_by_half f2
            where f2.client_code = f1.client_code
                and f2.source_system = f1.source_system
                and f2.court_id = f1.court_id
                and f2.hour_start = f1.hour_start
                and f2.half_position = 'second_half'
                and f2.period_type = f1.period_type
                and f1.slot_end = f2.slot_start  -- Ensure they're consecutive
        )
),

-- Second half orphans: courts with 5:30-6:00 that DON'T have 5:00-5:30 on the same court
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
        -- Exclude if this court has the first half (it forms a full hour, so don't show as orphan)
        -- Check that slots are consecutive (first half ends exactly when second half starts)
        and not exists (
            select 1
            from slots_by_half f1
            where f1.client_code = f2.client_code
                and f1.source_system = f2.source_system
                and f1.court_id = f2.court_id
                and f1.hour_start = f2.hour_start
                and f1.half_position = 'first_half'
                and f1.period_type = f2.period_type
                and f1.slot_end = f2.slot_start  -- Ensure they're consecutive
        )
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

-- Combine 2-hour slots, 1-hour slots, cross-court hour slots, and orphan slots
all_slots as (
    select
        client_code,
        source_system,
        slot_start,
        slot_end,
        available_courts_count,
        available_courts,
        period_type,
        'two_hour' as slot_type
    from two_hour_availability
    
    union all
    
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
    
    -- Cross-court hour slots (orphaned 30-min slots combined across different courts)
    select
        client_code,
        source_system,
        slot_start,
        slot_end,
        available_courts_count,
        available_courts,
        period_type,
        'cross_court_hour' as slot_type
    from cross_court_hour_slots
    
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
        (extract(epoch from (slot_end - slot_start)) / 60)::integer as duration_minutes,
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
    -- Convert 'NULL' back to NULL for period_type
    case when period_type = 'NULL' then null else period_type end as period_type,
    slot_type
from slots_with_duration
order by slot_start, slot_end, slot_type
