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
        start = self._get_utc_datetime(start)
        end = self._get_utc_datetime(end)
        print(
            f"[API CALL] GET /api/v1/member/get | "
            f"window={start.date()} to {end.date()} | "
            f"page={page_number} | page_size={page_size}"
        )
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
        data = resp.json()["Data"]
        members_count = len(data.get("Members", []))
        total_pages = data.get("TotalPages", 1)
        print(
            f"[API RESPONSE] GET /api/v1/member/get | "
            f"window={start.date()} to {end.date()} | "
            f"page={page_number} | records_returned={members_count} | "
            f"total_pages={total_pages}"
        )
        return data

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
        window_num = 0
        for window_start in self._generate_date(start, record_window_days):
            if window_start > now:
                break
            window_num += 1
            window_end = min(
                self._get_utc_datetime(datetime.now()),
                window_start + timedelta(days=record_window_days),
            )

            print(
                f"\n[COURTRESERVE MEMBERS] Processing date window {window_num}: "
                f"{window_start.date()} to {window_end.date()} "
                f"(so far {len(members)} total members)"
            )

            page_number = 1
            while True:
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

                print(
                    f"[COURTRESERVE MEMBERS] Window {window_num} page {page_number}: "
                    f"added {len(page_members)} members | "
                    f"total so far: {len(members)}"
                )

                if max_results and len(members) >= max_results:
                    print(
                        f"[COURTRESERVE MEMBERS] Reached max_results={max_results}, "
                        f"stopping pagination"
                    )
                    return members[:max_results]

                total_pages = page.get("TotalPages") or 1
                if page_number >= total_pages:
                    print(
                        f"[COURTRESERVE MEMBERS] Window {window_num} complete: "
                        f"reached last page ({total_pages})"
                    )
                    break
                page_number += 1

        print(
            f"\n[COURTRESERVE MEMBERS] All windows complete: {len(members)} total members"
        )
        return members

    # Non-Event Reservations
    def get_reservations_by_updated_date(
        self,
        watermark: date,
        include_user_defined_fields: bool = False,
    ) -> dict:
        """
        Get reservations from CourtReserve API using incremental loading.
        Filters by createdOrUpdatedOn for incremental ELT processes.

        Args:
            watermark: Start date for incremental loads - filters by createdOrUpdatedOn
            include_user_defined_fields: Include user defined fields
        """
        print("Get reservations by updated date")
        url = f"{self.BASE_URL}/api/v1/reservationreport/listactive"

        reservations = []
        record_window_days = 7
        watermark = watermark.replace(hour=0, minute=0, second=0, microsecond=0)

        for start_date in self._generate_date(watermark, record_window_days):
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
            data = resp.json().get("Data") or []
            reservations.extend(data)
            print(
                f"Start date {start_date} End date {end_date} Appended {len(data)} rows"
            )

        return reservations

    def get_reservations_by_start_date(
        self,
        start_date: datetime,
        end_date: datetime,
        include_user_defined_fields: bool = False,
    ) -> list[dict]:
        """
        Get reservations filtered by reservation start time (StartTime).
        Used for court availability calculation.

        Args:
            start_date: Filter reservations where StartTime >= this date
            end_date: Filter reservations where StartTime <= this date
            include_user_defined_fields: Include user defined fields

        Returns:
            List of reservation dictionaries
        """
        url = f"{self.BASE_URL}/api/v1/reservationreport/listactive"

        start_date = self._get_utc_datetime(start_date)
        end_date = self._get_utc_datetime(end_date)

        params = {
            "reservationFromDate": start_date.strftime("%Y-%m-%d"),
            "reservationToDate": end_date.strftime("%Y-%m-%d"),
            "includeUserDefinedFields": include_user_defined_fields,
        }

        print(
            f"[API CALL] GET /api/v1/reservationreport/listactive | "
            f"reservationFromDate={params['reservationFromDate']} | "
            f"reservationToDate={params['reservationToDate']}"
        )

        resp = requests.get(url, params=params, auth=self.auth)
        resp.raise_for_status()
        data = resp.json().get("Data") or []

        print(
            f"[API RESPONSE] Retrieved {len(data)} reservations with start time in range"
        )

        return data

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
        return resp.json().get("Data") or []

    def get_events(
        self,
        start_date: datetime,
        end_date: datetime,
        *,
        category_id: Optional[int] = None,
        category_ids: Optional[str] = None,
        include_registered_players_count: bool = True,
        include_price_info: bool = True,
        include_rating_restrictions: bool = True,
        include_tags: bool = True,
        event_filter_name: Optional[str] = None,
        event_filter_id: Optional[int] = None,
        tag_names: Optional[str] = None,
        tag_ids: Optional[str] = None,
    ) -> list[dict]:
        """
        Get events from CourtReserve API.

        Args:
            start_date: Start date for events
            end_date: End date for events
            category_id: Single category ID to filter by
            category_ids: Comma-separated category IDs
            include_registered_players_count: Include registered players count
            include_price_info: Include price information
            include_rating_restrictions: Include rating restrictions
            include_tags: Include tags
            event_filter_name: Filter by event filter name
            event_filter_id: Filter by event filter ID
            tag_names: Filter by tag names
            tag_ids: Filter by tag IDs
        """
        url = f"{self.BASE_URL}/api/v1/eventcalendar/eventlist"

        start_date = self._get_utc_datetime(start_date)
        end_date = self._get_utc_datetime(end_date)

        print(
            f"[API CALL] GET /api/v1/eventcalendar/eventlist | "
            f"startDate={start_date.date()} | endDate={end_date.date()}"
        )

        params = {
            "startDate": start_date.strftime("%Y-%m-%d"),
            "endDate": end_date.strftime("%Y-%m-%d"),
            "includeRegisteredPlayersCount": include_registered_players_count,
            "includePriceInfo": include_price_info,
            "includeRatingRestrictions": include_rating_restrictions,
            "includeTags": include_tags,
        }

        if category_id:
            params["categoryId"] = category_id
        if category_ids:
            params["categoryIds"] = category_ids
        if event_filter_name:
            params["eventFilterName"] = event_filter_name
        if event_filter_id:
            params["eventFilterId"] = event_filter_id
        if tag_names:
            params["tagNames"] = tag_names
        if tag_ids:
            params["tagIds"] = tag_ids

        resp = requests.get(url, params=params, auth=self.auth)
        resp.raise_for_status()

        error_message = resp.json().get("ErrorMessage")
        success_status = resp.json().get("IsSuccessStatusCode")
        if error_message or not success_status:
            raise Exception(f"CourtReserve API error: {error_message}")

        events = resp.json().get("Data", [])
        print(
            f"[API RESPONSE] GET /api/v1/eventcalendar/eventlist | "
            f"events_returned={len(events)}"
        )

        return events
