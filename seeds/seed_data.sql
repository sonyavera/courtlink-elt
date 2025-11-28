-- Idempotent seed data for courtlink-elt
-- This file can be run multiple times without causing errors
-- Uses :schema placeholder which will be replaced with PG_SCHEMA env var

-- ============================================================================
-- ORGANIZATIONS
-- ============================================================================

INSERT INTO :schema.organizations (
    id,
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
) VALUES 
    (
        1,
        'courtreserve',
        'pklyn',
        'https://app.courtreserve.com/Online/Account/LogIn/11868',
        NULL,
        true,
        110,
        90,
        90,
        88,
        'PKLYN',
        '{"weekday_peak": null, "weekend_peak": null, "weekday_off_peak": null}'::jsonb,
        'Gowanus',
        NULL,
        NULL,
        '{
            "timezone": "America/New_York",
            "monday": {"open": "07:00", "close": "23:00"},
            "tuesday": {"open": "07:00", "close": "23:00"},
            "wednesday": {"open": "07:00", "close": "23:00"},
            "thursday": {"open": "07:00", "close": "23:00"},
            "friday": {"open": "07:00", "close": "23:00"},
            "saturday": {"open": "07:00", "close": "23:00"},
            "sunday": {"open": "07:00", "close": "23:00"}
        }'::jsonb
    ),
    (
        2,
        'podplay',
        'redhookpickleball',
        'https://redhookpickleball.podplay.app/login?redirect=%2Fapp-menus&loginMode=password',
        'brooklyn',
        false,
        99,
        75,
        79,
        60,
        'Red Hook Pickleball Club',
        '{"timezone": "America/New_York", "weekday_peak": [{"end": "10:00", "start": "07:00"}, {"end": "22:00", "start": "17:00"}], "weekend_peak": null, "weekday_off_peak": [{"end": "17:00", "start": "10:00"}]}'::jsonb,
        'Red Hook',
        NULL,
        NULL,
        NULL
    ),
    (
        3,
        'podplay',
        'goodland',
        'https://goodland.podplay.app/login?redirect=%2Fapp-menus&loginMode=password',
        'greenpoint-indoor',
        false,
        110,
        66,
        80,
        48,
        'Goodland',
        '{"weekday_peak": null, "weekend_peak": null, "weekday_off_peak": null}'::jsonb,
        'Greenpoint',
        'greenpoint-indoor-1',
        NULL,
        NULL
    ),
    (
        4,
        'podplay',
        'gotham',
        'https://gotham.podplay.app/login?redirect=%2Fapp-menus&loginMode=password',
        'long-island-city',
        true,
        110,
        88,
        80,
        60,
        'Gotham',
        '{"timezone": "America/New_York", "weekday_peak": [{"end": "10:00", "start": "07:00"}, {"end": "22:00", "start": "17:00"}], "weekend_peak": null, "weekday_off_peak": [{"end": "17:00", "start": "10:00"}]}'::jsonb,
        'Long Island City',
        NULL,
        'e6e67f0a-de3f-4e6e-adf5-3db126cd5c83',
        '{
            "timezone": "America/New_York",
            "monday": {"open": "05:00", "close": "01:00"},
            "tuesday": {"open": "05:00", "close": "01:00"},
            "wednesday": {"open": "05:00", "close": "01:00"},
            "thursday": {"open": "05:00", "close": "01:00"},
            "friday": {"open": "05:00", "close": "01:00"},
            "saturday": {"open": "05:00", "close": "01:00"},
            "sunday": {"open": "05:00", "close": "01:00"}
        }'::jsonb
    ),
    (
        5,
        'podplay',
        'citypickle',
        'https://citypickle.podplay.app/login?redirect=%2Fapp-menus&loginMode=password',
        'long-island',
        false,
        99,
        79,
        50,
        40,
        'CityPickle',
        '{"timezone": "America/New_York", "weekday_peak": [{"end": "10:00", "start": "07:00"}, {"end": "22:00", "start": "17:00"}], "weekend_peak": null, "weekday_off_peak": null}'::jsonb,
        'Long Island City',
        'long-island-open',
        NULL,
        NULL
    )
ON CONFLICT (id) DO UPDATE SET
    source_system_code = EXCLUDED.source_system_code,
    client_code = EXCLUDED.client_code,
    login_link = EXCLUDED.login_link,
    city = EXCLUDED.city,
    is_customer = EXCLUDED.is_customer,
    hourly_rate_non_member = EXCLUDED.hourly_rate_non_member,
    hourly_rate_member = EXCLUDED.hourly_rate_member,
    hourly_rate_non_member_off_peak = EXCLUDED.hourly_rate_non_member_off_peak,
    hourly_rate_member_off_peak = EXCLUDED.hourly_rate_member_off_peak,
    facility_display_name = EXCLUDED.facility_display_name,
    peak_hours = EXCLUDED.peak_hours,
    location_display_name = EXCLUDED.location_display_name,
    podplay_pod = EXCLUDED.podplay_pod,
    podplay_pod_id = EXCLUDED.podplay_pod_id,
    operating_hours = EXCLUDED.operating_hours;


-- ============================================================================
-- COURTS
-- ============================================================================

INSERT INTO :schema.courts (
    id,
    client_code,
    label,
    type_name,
    order_index,
    created_at,
    updated_at
) VALUES 
    (44076, 'pklyn', 'Court #1', 'Pickleball', 1, '2025-11-17 11:33:38.829405-05', '2025-11-17 11:33:38.829405-05'),
    (44077, 'pklyn', 'Court #2', 'Pickleball', 2, '2025-11-17 11:33:38.829405-05', '2025-11-17 11:33:38.829405-05'),
    (44078, 'pklyn', 'Court #3', 'Pickleball', 3, '2025-11-17 11:33:38.829405-05', '2025-11-17 11:33:38.829405-05'),
    (44079, 'pklyn', 'Court #4', 'Pickleball', 4, '2025-11-17 11:33:38.829405-05', '2025-11-17 11:33:38.829405-05'),
    (44080, 'pklyn', 'Court #5', 'Pickleball', 5, '2025-11-17 11:33:38.829405-05', '2025-11-17 11:33:38.829405-05')
ON CONFLICT (id) DO UPDATE SET
    client_code = EXCLUDED.client_code,
    label = EXCLUDED.label,
    type_name = EXCLUDED.type_name,
    order_index = EXCLUDED.order_index,
    updated_at = EXCLUDED.updated_at;

-- ============================================================================
-- SUMMARY
-- ============================================================================
-- Organizations: 5 rows (1 CourtReserve, 4 Podplay)
-- Courts: 5 rows (all PKLYN)
--
-- Operating Hours (stored in local time with timezone):
--   - PKLYN: 7am-11pm (America/New_York timezone)
--   - Gotham: 5am-1am (America/New_York timezone)
--
-- Peak Hours (stored in local time with timezone):
--   - Peak: 7am-10am and 5pm-10pm (America/New_York timezone)
--   - Off-Peak: 10am-5pm (America/New_York timezone)
--
-- Note: Times are stored in local facility timezone (America/New_York) with timezone metadata.
-- This automatically handles DST transitions - no need to update seed file twice a year.
-- The ingestion and dbt models convert to UTC when needed for comparisons.
-- When close time is less than open time, it crosses to the next day.
-- ============================================================================

