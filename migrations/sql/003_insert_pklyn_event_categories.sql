-- Insert PKLYN event categories
-- Run this after applying migration 424b0dd15504

INSERT INTO {{SCHEMA}}.facility_event_categories (client_code, source_system, id, event_category_name)
VALUES
    ('pklyn', 'courtreserve', '62948', 'PKLYN Special Events'),
    ('pklyn', 'courtreserve', '67580', 'Junior Programming'),
    ('pklyn', 'courtreserve', '45387', 'Classes'),
    ('pklyn', 'courtreserve', '45069', 'Drop-in'),
    ('pklyn', 'courtreserve', '45380', 'Compete'),
    ('pklyn', 'courtreserve', '66295', 'DUPR Assessment'),
    ('pklyn', 'courtreserve', '62947', 'Ping-Pong'),
    ('pklyn', 'courtreserve', '69927', 'Leagues'),
    ('pklyn', 'courtreserve', '70727', 'PKLYN Academy')
ON CONFLICT (client_code, source_system, id) DO UPDATE SET
    event_category_name = EXCLUDED.event_category_name;
