class EltWatermarks:
    RESERVATIONS = "reservations"
    RESERVATION_CANCELLATIONS = "reservation_cancellations"
    MEMBERS = "members"


class Tables:
    RESERVATIONS_RAW = "reservations_raw"
    RESERVATIONS_RAW_STG = "reservations_raw_stg"
    RESERVATION_CANCELLATIONS_RAW = "reservation_cancellations_raw"
    RESERVATION_CANCELLATIONS_RAW_STG = "reservation_cancellations_raw_stg"
    ELT_WATERMARKS = "elt_watermarks"
    MEMBERS_RAW = "members_raw"
    MEMBERS_RAW_STG = "members_raw_stg"
