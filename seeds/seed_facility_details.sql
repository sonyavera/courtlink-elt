-- Idempotent seed data for facility_details table
-- This file can be run multiple times without causing errors
-- Uses :schema placeholder which will be replaced with PG_SCHEMA env var

-- ============================================================================
-- FACILITY DETAILS
-- ============================================================================
-- Using separate INSERT statements with explicit column names for maintainability
-- This format makes it much easier to see which value goes to which column

-- PKLYN
INSERT INTO :schema.facility_details (
    client_code, street_address, city, state, zip_code, country,
    latitude, longitude, full_address, number_of_courts, indoor_outdoor,
    court_surface_type, has_showers, has_lounge_area, has_paddle_rentals,
    has_pro_shop, has_food_service, has_parking, parking_type, has_wifi,
    has_lockers, has_water_fountains, has_dink_court, has_workout_area,
    is_autonomous_facility, facility_type, year_opened, facility_size_sqft,
    amenities_list, notes, facility_metadata
) VALUES (
    'pklyn', '80 4th St', 'Brooklyn', 'NY', '11231', 'USA',
    40.67672573500147, -73.993297626984, '80 4th St, Brooklyn, NY 11231, USA',
    5, 'indoor', 'professional-grade asphalt',
    true, true, true, true, true, false, NULL, true, true, true, true, true,
    false, 'private', 2024, 18000, NULL, NULL, NULL
)
ON CONFLICT (client_code) DO UPDATE SET
    street_address = EXCLUDED.street_address, city = EXCLUDED.city,
    state = EXCLUDED.state, zip_code = EXCLUDED.zip_code, country = EXCLUDED.country,
    latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, full_address = EXCLUDED.full_address,
    number_of_courts = EXCLUDED.number_of_courts, indoor_outdoor = EXCLUDED.indoor_outdoor,
    court_surface_type = EXCLUDED.court_surface_type, has_showers = EXCLUDED.has_showers,
    has_lounge_area = EXCLUDED.has_lounge_area, has_paddle_rentals = EXCLUDED.has_paddle_rentals,
    has_pro_shop = EXCLUDED.has_pro_shop, has_food_service = EXCLUDED.has_food_service,
    has_parking = EXCLUDED.has_parking, parking_type = EXCLUDED.parking_type,
    has_wifi = EXCLUDED.has_wifi, has_lockers = EXCLUDED.has_lockers,
    has_water_fountains = EXCLUDED.has_water_fountains, has_dink_court = EXCLUDED.has_dink_court,
    has_workout_area = EXCLUDED.has_workout_area, is_autonomous_facility = EXCLUDED.is_autonomous_facility,
    facility_type = EXCLUDED.facility_type, year_opened = EXCLUDED.year_opened,
    facility_size_sqft = EXCLUDED.facility_size_sqft, amenities_list = EXCLUDED.amenities_list,
    notes = EXCLUDED.notes, facility_metadata = EXCLUDED.facility_metadata,
    updated_at = EXCLUDED.updated_at;

-- Gotham
INSERT INTO :schema.facility_details (
    client_code, street_address, city, state, zip_code, country,
    latitude, longitude, full_address, number_of_courts, indoor_outdoor,
    court_surface_type, has_showers, has_lounge_area, has_paddle_rentals,
    has_pro_shop, has_food_service, has_parking, parking_type, has_wifi,
    has_lockers, has_water_fountains, has_dink_court, has_workout_area,
    is_autonomous_facility, facility_type, year_opened, facility_size_sqft,
    amenities_list, notes, facility_metadata
) VALUES (
    'gotham', '5-25 46th Ave', 'Long Island City', 'NY', '11101', 'USA',
    40.74761843321619, -73.9542153194257, '5-25 46th Ave, Long Island City, NY 11101, USA',
    4, 'indoor', NULL,
    false, false, false, false, false, false, NULL, true, false, true, false, false,
    true, 'private', 2024, 7800, 'free_paddles_borrow', NULL, NULL
)
ON CONFLICT (client_code) DO UPDATE SET
    street_address = EXCLUDED.street_address, city = EXCLUDED.city,
    state = EXCLUDED.state, zip_code = EXCLUDED.zip_code, country = EXCLUDED.country,
    latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, full_address = EXCLUDED.full_address,
    number_of_courts = EXCLUDED.number_of_courts, indoor_outdoor = EXCLUDED.indoor_outdoor,
    court_surface_type = EXCLUDED.court_surface_type, has_showers = EXCLUDED.has_showers,
    has_lounge_area = EXCLUDED.has_lounge_area, has_paddle_rentals = EXCLUDED.has_paddle_rentals,
    has_pro_shop = EXCLUDED.has_pro_shop, has_food_service = EXCLUDED.has_food_service,
    has_parking = EXCLUDED.has_parking, parking_type = EXCLUDED.parking_type,
    has_wifi = EXCLUDED.has_wifi, has_lockers = EXCLUDED.has_lockers,
    has_water_fountains = EXCLUDED.has_water_fountains, has_dink_court = EXCLUDED.has_dink_court,
    has_workout_area = EXCLUDED.has_workout_area, is_autonomous_facility = EXCLUDED.is_autonomous_facility,
    facility_type = EXCLUDED.facility_type, year_opened = EXCLUDED.year_opened,
    facility_size_sqft = EXCLUDED.facility_size_sqft, amenities_list = EXCLUDED.amenities_list,
    notes = EXCLUDED.notes, facility_metadata = EXCLUDED.facility_metadata,
    updated_at = EXCLUDED.updated_at;

-- Red Hook Pickleball Club
INSERT INTO :schema.facility_details (
    client_code, street_address, city, state, zip_code, country,
    latitude, longitude, full_address, number_of_courts, indoor_outdoor,
    court_surface_type, has_showers, has_lounge_area, has_paddle_rentals,
    has_pro_shop, has_food_service, has_parking, parking_type, has_wifi,
    has_lockers, has_water_fountains, has_dink_court, has_workout_area,
    is_autonomous_facility, facility_type, year_opened, facility_size_sqft,
    amenities_list, notes, facility_metadata
) VALUES (
    'redhookpickleball', '262 Van Brunt St', 'Brooklyn', 'NY', '11231', 'USA',
    40.68070423296283, -74.0097362175778, '262 Van Brunt St, Brooklyn, NY 11231, USA',
    5, NULL, 'cushioned',
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL
)
ON CONFLICT (client_code) DO UPDATE SET
    street_address = EXCLUDED.street_address, city = EXCLUDED.city,
    state = EXCLUDED.state, zip_code = EXCLUDED.zip_code, country = EXCLUDED.country,
    latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, full_address = EXCLUDED.full_address,
    number_of_courts = EXCLUDED.number_of_courts, indoor_outdoor = EXCLUDED.indoor_outdoor,
    court_surface_type = EXCLUDED.court_surface_type, has_showers = EXCLUDED.has_showers,
    has_lounge_area = EXCLUDED.has_lounge_area, has_paddle_rentals = EXCLUDED.has_paddle_rentals,
    has_pro_shop = EXCLUDED.has_pro_shop, has_food_service = EXCLUDED.has_food_service,
    has_parking = EXCLUDED.has_parking, parking_type = EXCLUDED.parking_type,
    has_wifi = EXCLUDED.has_wifi, has_lockers = EXCLUDED.has_lockers,
    has_water_fountains = EXCLUDED.has_water_fountains, has_dink_court = EXCLUDED.has_dink_court,
    has_workout_area = EXCLUDED.has_workout_area, is_autonomous_facility = EXCLUDED.is_autonomous_facility,
    facility_type = EXCLUDED.facility_type, year_opened = EXCLUDED.year_opened,
    facility_size_sqft = EXCLUDED.facility_size_sqft, amenities_list = EXCLUDED.amenities_list,
    notes = EXCLUDED.notes, facility_metadata = EXCLUDED.facility_metadata,
    updated_at = EXCLUDED.updated_at;

-- Goodland
INSERT INTO :schema.facility_details (
    client_code, street_address, city, state, zip_code, country,
    latitude, longitude, full_address, number_of_courts, indoor_outdoor,
    court_surface_type, has_showers, has_lounge_area, has_paddle_rentals,
    has_pro_shop, has_food_service, has_parking, parking_type, has_wifi,
    has_lockers, has_water_fountains, has_dink_court, has_workout_area,
    is_autonomous_facility, facility_type, year_opened, facility_size_sqft,
    amenities_list, notes, facility_metadata
) VALUES (
    'goodland', '67 West St Suite 110', 'Brooklyn', 'NY', '11222', 'USA',
    40.728958221910496, -73.95995640408287, '67 West St Suite 110, Brooklyn, NY 11222, USA',
    4, NULL, 'professional-grade acrylic',
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, true,
    NULL, NULL, 12000, NULL, NULL, NULL
)
ON CONFLICT (client_code) DO UPDATE SET
    street_address = EXCLUDED.street_address, city = EXCLUDED.city,
    state = EXCLUDED.state, zip_code = EXCLUDED.zip_code, country = EXCLUDED.country,
    latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, full_address = EXCLUDED.full_address,
    number_of_courts = EXCLUDED.number_of_courts, indoor_outdoor = EXCLUDED.indoor_outdoor,
    court_surface_type = EXCLUDED.court_surface_type, has_showers = EXCLUDED.has_showers,
    has_lounge_area = EXCLUDED.has_lounge_area, has_paddle_rentals = EXCLUDED.has_paddle_rentals,
    has_pro_shop = EXCLUDED.has_pro_shop, has_food_service = EXCLUDED.has_food_service,
    has_parking = EXCLUDED.has_parking, parking_type = EXCLUDED.parking_type,
    has_wifi = EXCLUDED.has_wifi, has_lockers = EXCLUDED.has_lockers,
    has_water_fountains = EXCLUDED.has_water_fountains, has_dink_court = EXCLUDED.has_dink_court,
    has_workout_area = EXCLUDED.has_workout_area, is_autonomous_facility = EXCLUDED.is_autonomous_facility,
    facility_type = EXCLUDED.facility_type, year_opened = EXCLUDED.year_opened,
    facility_size_sqft = EXCLUDED.facility_size_sqft, amenities_list = EXCLUDED.amenities_list,
    notes = EXCLUDED.notes, facility_metadata = EXCLUDED.facility_metadata,
    updated_at = EXCLUDED.updated_at;

-- CityPickle
INSERT INTO :schema.facility_details (
    client_code, street_address, city, state, zip_code, country,
    latitude, longitude, full_address, number_of_courts, indoor_outdoor,
    court_surface_type, has_showers, has_lounge_area, has_paddle_rentals,
    has_pro_shop, has_food_service, has_parking, parking_type, has_wifi,
    has_lockers, has_water_fountains, has_dink_court, has_workout_area,
    is_autonomous_facility, facility_type, year_opened, facility_size_sqft,
    amenities_list, notes, facility_metadata
) VALUES (
    'citypickle', '9-03 44th Rd', 'Long Island City', 'NY', '11101', 'USA',
    40.75037973285398, -73.95089150000004, '9-03 44th Rd, Long Island City, NY 11101, USA',
    4, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL
)
ON CONFLICT (client_code) DO UPDATE SET
    street_address = EXCLUDED.street_address, city = EXCLUDED.city,
    state = EXCLUDED.state, zip_code = EXCLUDED.zip_code, country = EXCLUDED.country,
    latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, full_address = EXCLUDED.full_address,
    number_of_courts = EXCLUDED.number_of_courts, indoor_outdoor = EXCLUDED.indoor_outdoor,
    court_surface_type = EXCLUDED.court_surface_type, has_showers = EXCLUDED.has_showers,
    has_lounge_area = EXCLUDED.has_lounge_area, has_paddle_rentals = EXCLUDED.has_paddle_rentals,
    has_pro_shop = EXCLUDED.has_pro_shop, has_food_service = EXCLUDED.has_food_service,
    has_parking = EXCLUDED.has_parking, parking_type = EXCLUDED.parking_type,
    has_wifi = EXCLUDED.has_wifi, has_lockers = EXCLUDED.has_lockers,
    has_water_fountains = EXCLUDED.has_water_fountains, has_dink_court = EXCLUDED.has_dink_court,
    has_workout_area = EXCLUDED.has_workout_area, is_autonomous_facility = EXCLUDED.is_autonomous_facility,
    facility_type = EXCLUDED.facility_type, year_opened = EXCLUDED.year_opened,
    facility_size_sqft = EXCLUDED.facility_size_sqft, amenities_list = EXCLUDED.amenities_list,
    notes = EXCLUDED.notes, facility_metadata = EXCLUDED.facility_metadata,
    updated_at = EXCLUDED.updated_at;

-- Velto
INSERT INTO :schema.facility_details (
    client_code, street_address, city, state, zip_code, country,
    latitude, longitude, full_address, number_of_courts, indoor_outdoor,
    court_surface_type, has_showers, has_lounge_area, has_paddle_rentals,
    has_pro_shop, has_food_service, has_parking, parking_type, has_wifi,
    has_lockers, has_water_fountains, has_dink_court, has_workout_area,
    is_autonomous_facility, facility_type, year_opened, facility_size_sqft,
    amenities_list, notes, facility_metadata
) VALUES (
    'velto', '160 Van Brunt St', 'Brooklyn', 'NY', '11231', 'USA',
    40.700023165963024, -74.00285361760457, '160 Van Brunt St, Brooklyn, NY 11231, USA',
    NULL, NULL, 'cushioned',
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, true,
    NULL, NULL, 14000, NULL, NULL, NULL
)
ON CONFLICT (client_code) DO UPDATE SET
    street_address = EXCLUDED.street_address, city = EXCLUDED.city,
    state = EXCLUDED.state, zip_code = EXCLUDED.zip_code, country = EXCLUDED.country,
    latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, full_address = EXCLUDED.full_address,
    number_of_courts = EXCLUDED.number_of_courts, indoor_outdoor = EXCLUDED.indoor_outdoor,
    court_surface_type = EXCLUDED.court_surface_type, has_showers = EXCLUDED.has_showers,
    has_lounge_area = EXCLUDED.has_lounge_area, has_paddle_rentals = EXCLUDED.has_paddle_rentals,
    has_pro_shop = EXCLUDED.has_pro_shop, has_food_service = EXCLUDED.has_food_service,
    has_parking = EXCLUDED.has_parking, parking_type = EXCLUDED.parking_type,
    has_wifi = EXCLUDED.has_wifi, has_lockers = EXCLUDED.has_lockers,
    has_water_fountains = EXCLUDED.has_water_fountains, has_dink_court = EXCLUDED.has_dink_court,
    has_workout_area = EXCLUDED.has_workout_area, is_autonomous_facility = EXCLUDED.is_autonomous_facility,
    facility_type = EXCLUDED.facility_type, year_opened = EXCLUDED.year_opened,
    facility_size_sqft = EXCLUDED.facility_size_sqft, amenities_list = EXCLUDED.amenities_list,
    notes = EXCLUDED.notes, facility_metadata = EXCLUDED.facility_metadata,
    updated_at = EXCLUDED.updated_at;

