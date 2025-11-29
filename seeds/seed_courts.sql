-- Idempotent seed data for courts table
-- This file can be run multiple times without causing errors
-- Uses :schema placeholder which will be replaced with PG_SCHEMA env var

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

