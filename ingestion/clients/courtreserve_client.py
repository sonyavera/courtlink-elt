import requests
from requests.auth import HTTPBasicAuth
from datetime import date, datetime, timedelta, timezone
from typing import Iterator, Optional


class CourtReserveClient:
    BASE_URL = "https://api.courtreserve.com"

    def __init__(self, username: str, password: str):
        self.auth = HTTPBasicAuth(username, password)

    def _get_utc_datetime(self, d) -> date:
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        else:
            d = d.astimezone(timezone.utc)

        return d

    def _generate_date(self, start_date, steps_in_days) -> Iterator[date]:
        start_date = self._get_utc_datetime(start_date)

        while True:
            yield start_date
            start_date += timedelta(days=steps_in_days)

    def get_members_page(
        self,
        start: datetime,
        end: datetime,
        page_size,
        page_number: int = 1,
        include_user_defined_fields: bool = True,
        include_ratings: bool = True,
    ) -> dict:
        url = f"{self.BASE_URL}/api/v1/member/get"
        print("Fetching page", page_number)
        start = self._get_utc_datetime(start)
        end = self._get_utc_datetime(end)
        params = {
            "pageNumber": page_number,
            "pageSize": page_size,
            "includeUserDefinedFields": include_user_defined_fields,
            "includeRatings": include_ratings,
            "createdOrUpdatedFrom": start.isoformat(),
            "createdOrUpdatedTo": end.isoformat(),
        }
        resp = requests.get(url, params=params, auth=self.auth)
        resp.raise_for_status()
        error_message = resp.json().get("ErrorMessage")
        success_status = resp.json().get("IsSuccessStatusCode")
        if error_message or not success_status:
            raise Exception(f"CourtReserve API error: {error_message}")
        return resp.json()["Data"]

    def get_members_since(
        self,
        start: datetime,
        *,
        record_window_days: int = 21,
        page_size: int = 500,
        include_user_defined_fields: bool = True,
        include_ratings: bool = True,
        max_results: Optional[int] = None,
    ) -> list[dict]:
        start = self._get_utc_datetime(start)
        now = self._get_utc_datetime(datetime.now())

        members: list[dict] = []
        for window_start in self._generate_date(start, record_window_days):
            if window_start > now:
                break
            window_end = min(
                self._get_utc_datetime(datetime.now()),
                window_start + timedelta(days=record_window_days),
            )

            page_number = 1
            while True:
                print(
                    f"[CourtReserveClient] members window {window_start} to {window_end} "
                    f"page={page_number} page_size={page_size} (so far {len(members)} records)"
                )
                page = self.get_members_page(
                    start=window_start,
                    end=window_end,
                    page_size=page_size,
                    page_number=page_number,
                    include_user_defined_fields=include_user_defined_fields,
                    include_ratings=include_ratings,
                )
                page_members = page.get("Members", [])
                members.extend(page_members)

                if max_results and len(members) >= max_results:
                    return members[:max_results]

                total_pages = page.get("TotalPages") or 1
                if page_number >= total_pages:
                    break
                page_number += 1

        return members

    # Non-Event Reservations
    def get_reservations(
        self,
        elt_watermarket_reservations_events: date,
        include_user_defined_fields: bool = False,
    ) -> dict:
        print("Get reservations")
        url = f"{self.BASE_URL}/api/v1/reservationreport/listactive"

        reservations = []
        record_window_days = 7
        elt_watermarket_reservations_events = (
            elt_watermarket_reservations_events.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        )

        for start_date in self._generate_date(
            elt_watermarket_reservations_events, record_window_days
        ):
            print(f"Start date {start_date}")
            if start_date > datetime.now(timezone.utc):
                print("break")
                break
            end_date = min(
                self._get_utc_datetime(datetime.now()),
                start_date + timedelta(days=record_window_days),
            )
            params = {
                "createdOrUpdatedOnFrom": start_date.isoformat(),
                "createdOrUpdatedOnTo": end_date.isoformat(),
                "includeUserDefinedFields": include_user_defined_fields,
            }
            resp = requests.get(url, params=params, auth=self.auth)
            resp.raise_for_status()
            reservations.extend(resp.json()["Data"]) or []
            print(
                f"Start date {start_date} End date {end_date} Appended {len(resp.json()['Data'])} rows"
            )

        return reservations

    def get_reservation_cancellations(
        self, elt_watermark_event_cancellations: date
    ) -> list[dict]:
        print("Get event cancellations")

        url = f"{self.BASE_URL}/api/v1/reservationreport/listcancelled"

        params = {
            "cancelledOnFrom": elt_watermark_event_cancellations.isoformat(),
            "cancelledOnTo": datetime.now(timezone.utc).isoformat(),
        }
        resp = requests.get(url, params=params, auth=self.auth)
        resp.raise_for_status()
        return resp.json().get("Data", [])
