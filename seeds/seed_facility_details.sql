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
    amenities_list, notes, facility_metadata, facility_header_image_url, facility_logo_image_url
) VALUES (
    'pklyn', '80 4th St', 'Brooklyn', 'NY', '11231', 'USA',
    40.67672573500147, -73.993297626984, '80 4th St, Brooklyn, NY 11231, USA',
    5, 'indoor', 'professional-grade asphalt',
    true, true, true, true, true, false, NULL, true, true, true, true, true,
    false, 'private', 2024, 18000, NULL, NULL, NULL,
    'https://lh3.googleusercontent.com/p/AF1QipN6NARCB_dAwtDfuGY2DwSlydym_ZCtUNa1zcLp=w1800-h1689-p-k-no',
    'https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTsw8jRFO8YYigq9p6pAuq5rkw77xxuc3G9xg&s'
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
    facility_header_image_url = EXCLUDED.facility_header_image_url,
    facility_logo_image_url = EXCLUDED.facility_logo_image_url,
    updated_at = EXCLUDED.updated_at;

-- Gotham
INSERT INTO :schema.facility_details (
    client_code, street_address, city, state, zip_code, country,
    latitude, longitude, full_address, number_of_courts, indoor_outdoor,
    court_surface_type, has_showers, has_lounge_area, has_paddle_rentals,
    has_pro_shop, has_food_service, has_parking, parking_type, has_wifi,
    has_lockers, has_water_fountains, has_dink_court, has_workout_area,
    is_autonomous_facility, facility_type, year_opened, facility_size_sqft,
    amenities_list, notes, facility_metadata, facility_header_image_url, facility_logo_image_url
) VALUES (
    'gotham', '5-25 46th Ave', 'Long Island City', 'NY', '11101', 'USA',
    40.74761843321619, -73.9542153194257, '5-25 46th Ave, Long Island City, NY 11101, USA',
    4, 'indoor', NULL,
    false, false, true, false, false, false, NULL, true, false, true, false, false,
    true, 'private', 2024, 7800, 'free_paddles_borrow', NULL, NULL,
    'https://images.squarespace-cdn.com/content/v1/66dfc89620ff3e2b13e1beb6/1eb7e636-a5cf-45af-9b60-6c7b782d7544/GothamPickleball-02.jpg',
    'https://play-lh.googleusercontent.com/b7ltDzguuJDkgKgspUBDHHHvd53E4vX19xnP7RW-H7z6cE_G8Bihsner9hoS66Bzwt0=w240-h480-rw'
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
    facility_header_image_url = EXCLUDED.facility_header_image_url,
    facility_logo_image_url = EXCLUDED.facility_logo_image_url,
    updated_at = EXCLUDED.updated_at;

-- Red Hook Pickleball Club
INSERT INTO :schema.facility_details (
    client_code, street_address, city, state, zip_code, country,
    latitude, longitude, full_address, number_of_courts, indoor_outdoor,
    court_surface_type, has_showers, has_lounge_area, has_paddle_rentals,
    has_pro_shop, has_food_service, has_parking, parking_type, has_wifi,
    has_lockers, has_water_fountains, has_dink_court, has_workout_area,
    is_autonomous_facility, facility_type, year_opened, facility_size_sqft,
    amenities_list, notes, facility_metadata, facility_header_image_url, facility_logo_image_url
) VALUES (
    'redhookpickleball', '262 Van Brunt St', 'Brooklyn', 'NY', '11231', 'USA',
    40.68070423296283, -74.0097362175778, '262 Van Brunt St, Brooklyn, NY 11231, USA',
    5, NULL, 'cushioned',
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, true, NULL, NULL,
    NULL, NULL, NULL, 18000, NULL, NULL, NULL,
    'https://images.squarespace-cdn.com/content/v1/67e6d8c918fdc17910ec6c0f/20f13332-dfac-4886-a74b-66db05ac0d93/ZV103171.JPG',
    'https://images.squarespace-cdn.com/content/v1/67e6d8c918fdc17910ec6c0f/d61dfe2e-4a2e-4318-8ea7-19a049b208cc/RHPC_03.png'
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
    facility_header_image_url = EXCLUDED.facility_header_image_url,
    facility_logo_image_url = EXCLUDED.facility_logo_image_url,
    updated_at = EXCLUDED.updated_at;

-- Goodland
INSERT INTO :schema.facility_details (
    client_code, street_address, city, state, zip_code, country,
    latitude, longitude, full_address, number_of_courts, indoor_outdoor,
    court_surface_type, has_showers, has_lounge_area, has_paddle_rentals,
    has_pro_shop, has_food_service, has_parking, parking_type, has_wifi,
    has_lockers, has_water_fountains, has_dink_court, has_workout_area,
    is_autonomous_facility, facility_type, year_opened, facility_size_sqft,
    amenities_list, notes, facility_metadata, facility_header_image_url, facility_logo_image_url
) VALUES (
    'goodland', '67 West St Suite 110', 'Brooklyn', 'NY', '11222', 'USA',
    40.728958221910496, -73.95995640408287, '67 West St Suite 110, Brooklyn, NY 11222, USA',
    4, NULL, 'professional-grade acrylic',
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, true,
    NULL, NULL, 12000, NULL, NULL, NULL,
    'https://lh3.googleusercontent.com/p/AF1QipPVt5diCWgyujSFTfFCqlpvXJ-3XLPvPXGnOc4s=s1360-w1360-h1020-rw',
    'https://goodlandpickleball.com/cdn/shop/files/Beige_Cream_Elegant_Feminine_Handwriting_Circular_Badge_Wedding_Event_Logo.png?v=1717362897'
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
    facility_header_image_url = EXCLUDED.facility_header_image_url,
    facility_logo_image_url = EXCLUDED.facility_logo_image_url,
    updated_at = EXCLUDED.updated_at;

-- CityPickle
INSERT INTO :schema.facility_details (
    client_code, street_address, city, state, zip_code, country,
    latitude, longitude, full_address, number_of_courts, indoor_outdoor,
    court_surface_type, has_showers, has_lounge_area, has_paddle_rentals,
    has_pro_shop, has_food_service, has_parking, parking_type, has_wifi,
    has_lockers, has_water_fountains, has_dink_court, has_workout_area,
    is_autonomous_facility, facility_type, year_opened, facility_size_sqft,
    amenities_list, notes, facility_metadata, facility_header_image_url, facility_logo_image_url
) VALUES (
    'citypickle', '9-03 44th Rd', 'Long Island City', 'NY', '11101', 'USA',
    40.75037973285398, -73.95089150000004, '9-03 44th Rd, Long Island City, NY 11101, USA',
    4, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, 10000, NULL, NULL, NULL,
    'https://cdn.prod.website-files.com/62d5824b47b9d43a652ce731/654a521f63fae58de67b0e79_IC_Cover.jpg',
    'https://cdn.prod.website-files.com/65ef59977863009b87c1966d/65ef59977863009b87c1981c_gc.png'
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
    facility_header_image_url = EXCLUDED.facility_header_image_url,
    facility_logo_image_url = EXCLUDED.facility_logo_image_url,
    updated_at = EXCLUDED.updated_at;

-- Velto
INSERT INTO :schema.facility_details (
    client_code, street_address, city, state, zip_code, country,
    latitude, longitude, full_address, number_of_courts, indoor_outdoor,
    court_surface_type, has_showers, has_lounge_area, has_paddle_rentals,
    has_pro_shop, has_food_service, has_parking, parking_type, has_wifi,
    has_lockers, has_water_fountains, has_dink_court, has_workout_area,
    is_autonomous_facility, facility_type, year_opened, facility_size_sqft,
    amenities_list, notes, facility_metadata, facility_header_image_url, facility_logo_image_url
) VALUES (
    'velto', '160 Van Brunt St', 'Brooklyn', 'NY', '11231', 'USA',
    40.700023165963024, -74.00285361760457, '160 Van Brunt St, Brooklyn, NY 11231, USA',
    5, NULL, 'cushioned',
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, true,
    NULL, NULL, 14000, NULL, NULL, NULL, NULL,
    'https://veltopb.com/Velto_03_Logotype_Forest.png'
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
    facility_header_image_url = EXCLUDED.facility_header_image_url,
    facility_logo_image_url = EXCLUDED.facility_logo_image_url,
    updated_at = EXCLUDED.updated_at;

