"""
SQLAlchemy models for database migrations.

These models represent the database schema for Alembic migrations.

"""

from sqlalchemy import (
    Boolean,
    BigInteger,
    Column,
    Date,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    func,
    PrimaryKeyConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class EltWatermark(Base):
    """ELT watermark tracking table."""

    __tablename__ = "elt_watermarks"

    source_name = Column(Text, primary_key=True)
    last_loaded_at = Column(DateTime(timezone=True))
    last_record_created_at = Column(DateTime(timezone=True))

    __table_args__ = (
        Index("elt_watermarks_source_name_idx", "source_name", unique=True),
    )


class MemberRaw(Base):
    """Raw members table."""

    __tablename__ = "members_raw"

    client_code = Column(Text, nullable=False, primary_key=True)
    member_id = Column(Text, nullable=False, primary_key=True)
    first_name = Column(Text)
    last_name = Column(Text)
    gender = Column(Text)
    date_of_birth = Column(Date)
    phone_number = Column(Text)
    email = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("members_raw_client_member_idx", "client_code", "member_id", unique=True),
    )


class MemberRawStg(Base):
    """Staging table for members."""

    __tablename__ = "members_raw_stg"

    client_code = Column(Text, nullable=False, primary_key=True)
    member_id = Column(Text, nullable=False, primary_key=True)
    first_name = Column(Text)
    last_name = Column(Text)
    gender = Column(Text)
    date_of_birth = Column(Date)
    phone_number = Column(Text)
    club_member_key = Column(Text)
    email = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index(
            "members_raw_stg_client_member_idx", "client_code", "member_id", unique=True
        ),
    )


class ReservationRaw(Base):
    """Raw reservations table."""

    __tablename__ = "reservations_raw"

    client_code = Column(Text, nullable=False, primary_key=True)
    reservation_id = Column(Text, nullable=False, primary_key=True)
    event_id = Column(Text)
    member_id = Column(Text)
    reservation_created_at = Column(DateTime(timezone=True))
    reservation_updated_at = Column(DateTime(timezone=True))
    reservation_start_at = Column(DateTime(timezone=True))
    reservation_end_at = Column(DateTime(timezone=True))
    reservation_cancelled_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index(
            "reservations_raw_client_res_member_idx",
            "client_code",
            "reservation_id",
            "member_id",
            unique=True,
        ),
        PrimaryKeyConstraint("client_code", "reservation_id"),
    )


class ReservationRawStg(Base):
    """Staging table for reservations.

    Note: The model definition includes a primary key for SQLAlchemy compatibility,
    but the migration (fix_reservations_raw_stg_primary_key) removes this primary key
    from the actual database table to allow multiple people with the same reservation_id.
    The unique index on (client_code, reservation_id, member_id) prevents true duplicates.
    """

    __tablename__ = "reservations_raw_stg"

    client_code = Column(Text, nullable=False, primary_key=True)
    reservation_id = Column(Text, nullable=False, primary_key=True)
    event_id = Column(Text)
    member_id = Column(Text)
    reservation_created_at = Column(DateTime(timezone=True))
    reservation_updated_at = Column(DateTime(timezone=True))
    reservation_start_at = Column(DateTime(timezone=True))
    reservation_end_at = Column(DateTime(timezone=True))
    reservation_cancelled_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        # Primary key constraint defined here for SQLAlchemy compatibility
        # but will be removed by migration fix_reservations_raw_stg_primary_key
        PrimaryKeyConstraint("client_code", "reservation_id"),
        # Unique index on (client_code, reservation_id, member_id) to prevent true duplicates
        # Allows multiple people with the same reservation_id
        Index(
            "reservations_raw_stg_client_res_member_idx",
            "client_code",
            "reservation_id",
            "member_id",
            unique=True,
        ),
    )


class ReservationCancellationRaw(Base):
    """Raw reservation cancellations table."""

    __tablename__ = "reservation_cancellations_raw"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(Text)
    reservation_id = Column(Text)
    reservation_type = Column(Text)
    reservation_created_at = Column(DateTime(timezone=True))
    reservation_start_at = Column(DateTime(timezone=True))
    reservation_end_at = Column(DateTime(timezone=True))
    cancelled_on = Column(DateTime(timezone=True))
    day_of_week = Column(Text)
    is_program = Column(Boolean)
    program_name = Column(Text)
    player_name = Column(Text)
    player_first_name = Column(Text)
    player_last_name = Column(Text)
    player_email = Column(Text)
    player_phone = Column(Text)
    fee = Column(Numeric)
    is_team_event = Column(Boolean)
    event_category_name = Column(Text)
    event_category_id = Column(Text)
    member_id = Column(Text)
    client_code = Column(Text)
    source_system = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class FacilityEventCategory(Base):
    """Facility event categories table."""

    __tablename__ = "facility_event_categories"

    client_code = Column(Text, nullable=False, primary_key=True)
    source_system = Column(Text, nullable=False, primary_key=True)
    id = Column(Text, nullable=False, primary_key=True)
    event_category_name = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class FacilityEventRaw(Base):
    """Raw facility events table."""

    __tablename__ = "facility_events_raw"

    client_code = Column(Text, nullable=False, primary_key=True)
    source_system = Column(Text, nullable=False, primary_key=True)
    event_id = Column(Text, nullable=False, primary_key=True)
    event_start_time = Column(DateTime(timezone=True), nullable=False, primary_key=True)
    event_name = Column(Text)
    event_description = Column(Text)
    event_type = Column(Text)
    event_end_time = Column(DateTime(timezone=True))
    num_registrants = Column(Integer)
    max_registrants = Column(Integer)
    admission_rate_regular = Column(Numeric)
    admission_rate_member = Column(Numeric)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class FacilityCourtAvailability(Base):
    """Facility court availabilities table."""

    __tablename__ = "facility_court_availabilities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_code = Column(Text, nullable=False)
    source_system = Column(Text, nullable=False)
    court_id = Column(Text, nullable=False)
    court_name = Column(Text, nullable=True)
    slot_start = Column(DateTime(timezone=True), nullable=False)
    slot_end = Column(DateTime(timezone=True), nullable=False)
    period_type = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Organization(Base):
    """Organizations table."""

    __tablename__ = "organizations"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    source_system_code = Column(Text, nullable=False)
    client_code = Column(Text, nullable=False)
    login_link = Column(Text)
    city = Column(Text)
    is_customer = Column(Boolean)
    hourly_rate_non_member = Column(Integer)
    hourly_rate_member = Column(Integer)
    hourly_rate_non_member_off_peak = Column(Integer)
    hourly_rate_member_off_peak = Column(Integer)
    facility_display_name = Column(Text)
    peak_hours = Column(JSONB)
    location_display_name = Column(Text)
    podplay_pod = Column(Text)
    podplay_pod_id = Column(Text)
    operating_hours = Column(JSONB)
    google_place_id = Column(Text)
    google_photo_name = Column(Text)  # Photo name/reference (not a direct URL - requires API key header)


class FacilityDetails(Base):
    """Facility details and attributes table."""

    __tablename__ = "facility_details"

    client_code = Column(Text, primary_key=True)
    street_address = Column(Text)
    city = Column(Text)
    state = Column(Text)
    zip_code = Column(Text)
    country = Column(Text)
    latitude = Column(Numeric)
    longitude = Column(Numeric)
    full_address = Column(Text)
    number_of_courts = Column(Integer)
    primary_court_type = Column(Text)
    court_types = Column(JSONB)
    indoor_outdoor = Column(Text)
    court_surface_type = Column(Text)
    has_showers = Column(Boolean)
    has_lounge_area = Column(Boolean)
    has_paddle_rentals = Column(Boolean)
    has_pro_shop = Column(Boolean)
    has_food_service = Column(Boolean)
    has_parking = Column(Boolean)
    parking_type = Column(Text)
    has_wifi = Column(Boolean)
    has_lockers = Column(Boolean)
    has_water_fountains = Column(Boolean)
    has_dink_court = Column(Boolean)
    has_workout_area = Column(Boolean)
    is_autonomous_facility = Column(Boolean)
    facility_type = Column(Text)
    year_opened = Column(Integer)
    facility_size_sqft = Column(Integer)
    amenities_list = Column(Text)
    notes = Column(Text)
    facility_metadata = Column(JSONB)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class FacilityReview(Base):
    """Aggregate review data for facilities from various providers (Google, Yelp, etc.)."""

    __tablename__ = "facility_reviews"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    client_code = Column(Text, nullable=False)
    review_service = Column(Text, nullable=False)  # 'google', 'yelp', etc.
    num_reviews = Column(Integer)
    avg_review = Column(Numeric)
    link_to_reviews = Column(Text)
    last_updated_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        Index(
            "facility_reviews_client_service_idx",
            "client_code",
            "review_service",
            unique=True,
        ),
    )


class ReservationCancellationRawStg(Base):
    """Staging table for reservation cancellations."""

    __tablename__ = "reservation_cancellations_raw_stg"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(Text)
    reservation_id = Column(Text)
    reservation_type = Column(Text)
    reservation_created_at = Column(DateTime(timezone=True))
    reservation_start_at = Column(DateTime(timezone=True))
    reservation_end_at = Column(DateTime(timezone=True))
    cancelled_on = Column(DateTime(timezone=True))
    day_of_week = Column(Text)
    is_program = Column(Boolean)
    program_name = Column(Text)
    player_name = Column(Text)
    player_first_name = Column(Text)
    player_last_name = Column(Text)
    player_email = Column(Text)
    player_phone = Column(Text)
    fee = Column(Numeric)
    is_team_event = Column(Boolean)
    event_category_name = Column(Text)
    event_category_id = Column(Text)
    member_id = Column(Text)
    client_code = Column(Text)
    source_system = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Court(Base):
    """Courts table."""

    __tablename__ = "courts"

    id = Column(BigInteger, primary_key=True)
    client_code = Column(Text, nullable=False)
    label = Column(Text, nullable=False)
    type_name = Column(Text, nullable=False)
    order_index = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
