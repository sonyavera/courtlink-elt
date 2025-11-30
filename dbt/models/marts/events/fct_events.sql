{{ config(materialized='table', tags=['events']) }}

with base as (
    select 
        *
    from {{ source('raw', 'facility_events_raw') }}
),

mapped as (
    select
        client_code,
        source_system,
        event_id,
        event_name,
        event_description,
        event_type as event_type_original,
        case
            -- Classes: ADULT_CLASS, PKLYN Academy, PKLYN Special Events, Classes
            when upper(trim(event_type)) in ('ADULT_CLASS', 'PKLYN ACADEMY', 'PKLYN SPECIAL EVENTS', 'CLASSES')
                then 'Class'
            -- Open Play: OPEN_PLAY, Drop-in
            when upper(trim(event_type)) in ('OPEN_PLAY', 'DROP-IN', 'DROPIN')
                then 'Open Play'
            -- Compete: Compete, LEAGUE NIGHT, TOURNAMENT
            when upper(trim(event_type)) in ('COMPETE', 'LEAGUE NIGHT', 'TOURNAMENT')
                then 'Compete'
            -- Keep other types as-is (for now)
            else event_type
        end as event_type,
        -- Skill level is computed by Python preprocessing script (scripts/add_skill_level_to_events.py)
        -- and stored in facility_events_raw.skill_level
        coalesce(skill_level, 'All Levels') as skill_level,
        event_start_time,
        event_end_time,
        num_registrants,
        max_registrants,
        admission_rate_regular,
        admission_rate_member,
        created_at
    from base
    where
        -- Filter out PARTY and private event (case-insensitive)
        upper(trim(event_type)) not in ('PARTY', 'PRIVATE EVENT', 'PRIVATEEVENT')
)

select
    md5(
        concat_ws(
            '||',
            coalesce(client_code, ''),
            coalesce(event_id, ''),
            coalesce(source_system, ''),
            coalesce(cast(event_start_time as text), '')
        )
    ) as event_pk,
    client_code,
    source_system,
    event_id,
    event_name,
    event_description,
    event_type_original,
    event_type,
    skill_level,
    event_start_time,
    event_end_time,
    num_registrants,
    max_registrants,
    case
        when num_registrants is not null 
            and max_registrants is not null 
            and num_registrants >= max_registrants
        then true
        else false
    end as is_full,
    admission_rate_regular,
    admission_rate_member,
    created_at
from mapped

