from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional
import time

import requests


class PodplayClient:
    """Lightweight wrapper around the Podplay v2 REST API (x-api-key auth)."""

    BASE_URL = "https://gotham.podplay.app/apis/v2"
    DEFAULT_TIMEOUT_SECS = 30

    def __init__(self, api_key: str, timeout: int = DEFAULT_TIMEOUT_SECS):
        if not api_key:
            raise ValueError("Podplay API key is required")

        self.session = requests.Session()
        self.session.headers.update(
            {
                "x-api-key": api_key,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )
        self.timeout = timeout

    def _request(
        self, method: str, path: str, *, params: Optional[Dict] = None
    ) -> Dict:
        url = f"{self.BASE_URL}{path}"
        response = self.session.request(
            method,
            url,
            params=params,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _to_iso(dt: datetime) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def _paginate(
        self,
        path: str,
        *,
        params: Optional[Dict] = None,
        max_results: Optional[int] = None,
    ) -> Iterable[Dict]:
        base_params = {**(params or {})}
        page = 1
        yielded = 0

        while True:
            page_params = {**base_params, "page": page}
            # Log key params for debugging (don't log full expand array)
            log_params = {k: v for k, v in page_params.items() if k != "expand"}
            print(
                f"[API CALL] GET {path} | page={page_params.get('page')} | "
                f"ipp={page_params.get('ipp')} | startTime={log_params.get('startTime')} | "
                f"endTime={log_params.get('endTime')} | podId={log_params.get('podId')} | "
                f"yielded_so_far={yielded}"
            )
            payload = self._request("GET", path, params=page_params)
            items = payload.get("items", [])
            items_count = len(items)

            # Debug: Check if API is respecting ipp parameter
            requested_ipp = page_params.get("ipp")
            if items_count > requested_ipp:
                print(
                    f"[API WARNING] Requested ipp={requested_ipp} but got {items_count} items! "
                    f"API may not be respecting pagination parameter."
                )

            # Debug: Check date ranges in response (for sessions endpoint)
            if path == "/sessions" and items:
                from datetime import datetime, timezone

                start_times = []
                for item in items:
                    item_start = item.get("startTime")
                    if item_start:
                        try:
                            if isinstance(item_start, str):
                                if item_start.endswith("Z"):
                                    item_start_dt = datetime.fromisoformat(
                                        item_start.replace("Z", "+00:00")
                                    )
                                else:
                                    item_start_dt = datetime.fromisoformat(item_start)
                            else:
                                item_start_dt = item_start
                            start_times.append(item_start_dt)
                        except Exception:
                            pass

                if start_times:
                    earliest = min(start_times)
                    latest = max(start_times)
                    print(
                        f"[API DATE RANGE] Page {page} | earliest startTime: {earliest.isoformat()} | "
                        f"latest startTime: {latest.isoformat()} | requested endTime: {page_params.get('endTime')}"
                    )

                    # Check if latest exceeds our endTime
                    end_time_str = page_params.get("endTime")
                    if end_time_str:
                        try:
                            if isinstance(end_time_str, str):
                                if end_time_str.endswith("Z"):
                                    end_time_dt = datetime.fromisoformat(
                                        end_time_str.replace("Z", "+00:00")
                                    )
                                else:
                                    end_time_dt = datetime.fromisoformat(end_time_str)
                            else:
                                end_time_dt = end_time_str

                            if latest > end_time_dt:
                                print(
                                    f"[API DATE WARNING] Latest startTime {latest.isoformat()} EXCEEDS "
                                    f"requested endTime {end_time_str}! API may not be filtering by dates."
                                )
                        except Exception:
                            pass

            print(
                f"[API RESPONSE] GET {path} | page={page} | "
                f"records_returned={items_count} | total_yielded={yielded + items_count} | "
                f"requested_ipp={requested_ipp}"
            )

            # Small delay between API calls to avoid rate limiting (0.1s = ~10 req/sec max)
            if page > 1:  # Don't delay before first call
                time.sleep(0.1)

            if not items:
                print(f"[API PAGINATION] No more items returned, stopping pagination")
                break

            # For sessions endpoint, check if items exceed date range before yielding
            end_time_dt = None
            if path == "/sessions" and base_params.get("endTime"):
                try:
                    end_time_str = base_params.get("endTime")
                    if end_time_str:
                        from datetime import datetime, timezone

                        if isinstance(end_time_str, str):
                            if end_time_str.endswith("Z"):
                                end_time_dt = datetime.fromisoformat(
                                    end_time_str.replace("Z", "+00:00")
                                )
                            else:
                                end_time_dt = datetime.fromisoformat(end_time_str)
                        else:
                            end_time_dt = end_time_str
                except Exception as e:
                    print(
                        f"[API PAGINATION] Warning: Could not parse endTime for date filtering: {e}"
                    )

            for item in items:
                # Check date filtering for sessions endpoint
                if end_time_dt and path == "/sessions":
                    item_start = item.get("startTime")
                    if item_start:
                        try:
                            if isinstance(item_start, str):
                                if item_start.endswith("Z"):
                                    item_start_dt = datetime.fromisoformat(
                                        item_start.replace("Z", "+00:00")
                                    )
                                else:
                                    item_start_dt = datetime.fromisoformat(item_start)
                            else:
                                item_start_dt = item_start

                            if item_start_dt > end_time_dt:
                                print(
                                    f"[API PAGINATION] Found item with startTime {item_start} "
                                    f"exceeding endTime {base_params.get('endTime')}, stopping pagination"
                                )
                                return
                        except Exception:
                            pass

                yield item
                yielded += 1
                if max_results and yielded >= max_results:
                    print(
                        f"[API PAGINATION] Reached max_results={max_results}, "
                        f"stopping pagination"
                    )
                    return

            pagination = payload.get("_pagination") or {}
            ipp = pagination.get("ipp") or base_params.get("ipp")
            total = pagination.get("total")
            count = pagination.get("count")
            total_pages = (
                pagination.get("totalPages")
                or pagination.get("total_pages")
                or pagination.get("pages")
            )

            print(
                f"[API PAGINATION] page={page} | total={total} | count={count} | "
                f"ipp={ipp} | total_pages={total_pages}"
            )

            if total_pages and page >= total_pages:
                print(f"[API PAGINATION] Reached last page ({total_pages}), stopping")
                break
            if total and ipp and page * ipp >= total:
                print(f"[API PAGINATION] Reached total records ({total}), stopping")
                break
            if count is not None and ipp and count < ipp:
                print(
                    f"[API PAGINATION] Last page detected (count={count} < ipp={ipp}), "
                    f"stopping"
                )
                break

            page += 1

        print(f"[API SUMMARY] Total records yielded from {path}: {yielded}")

    def get_reservations(
        self,
        *,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        include_canceled: bool = True,
        page_size: int = 100,
        max_results: Optional[int] = None,
        expand: Optional[List[str]] = None,
        extra_filters: Optional[Dict] = None,
        event_type: Optional[str] = None,
    ) -> List[Dict]:
        params: Dict = {"ipp": page_size}

        if start_time:
            params["startTime"] = self._to_iso(start_time)
        if end_time:
            params["endTime"] = self._to_iso(end_time)
        if include_canceled:
            params["includeCanceled"] = True
        if expand:
            params["expand"] = expand
        if event_type:
            params["type"] = event_type
        if extra_filters:
            params.update(extra_filters)

        return list(
            self._paginate(
                "/events",
                params=params,
                max_results=max_results,
            )
        )

    def get_users(
        self,
        *,
        page_size: int = 500,
        max_results: Optional[int] = None,
        search: Optional[str] = None,
        role: Optional[List[str]] = None,
        expand: Optional[List[str]] = None,
        extra_filters: Optional[Dict] = None,
        member_since_min: Optional[datetime] = None,
        member_since_max: Optional[datetime] = None,
    ) -> List[Dict]:
        params: Dict = {"ipp": page_size}

        if search:
            params["search"] = search
        if role:
            params["role"] = role
        if expand:
            params["expand"] = expand
        if extra_filters:
            params.update(extra_filters)
        params["ipp"] = page_size
        if member_since_min:
            params["memberSinceMin"] = self._to_iso(member_since_min)
        if member_since_max:
            params["memberSinceMax"] = self._to_iso(member_since_max)

        return list(
            self._paginate(
                "/users",
                params=params,
                max_results=max_results,
            )
        )

    def get_events(
        self,
        *,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        event_types: Optional[List[str]] = None,
        pod_id: Optional[str] = None,
        page_size: int = 100,
        max_results: Optional[int] = None,
    ) -> List[Dict]:
        """
        Get events from Podplay API.

        Args:
            start_time: Start time for events (defaults to now)
            end_time: End time for events (defaults to start_time + 7 days)
            event_types: List of event types ("REGULAR", "CLASS", "EVENT")
            pod_id: Pod ID to filter events (from organizations.podplay_pod_id)
            page_size: Number of results per page
            max_results: Maximum number of results to return
        """
        params: Dict = {
            "ipp": page_size,
            "selfOnly": "true",
            "excludeClosedSeries": "true",
            "excludeUnlisted": "true",
            "includeCanceledInvitations": "false",
            "sort": "startTime",
        }

        if start_time:
            params["startTime"] = self._to_iso(start_time)
        if end_time:
            params["endTime"] = self._to_iso(end_time)
        if event_types:
            # Podplay API accepts type as array parameter - requests will handle list properly
            params["type"] = event_types
        if pod_id:
            params["podId"] = pod_id

        return list(
            self._paginate(
                "/events",
                params=params,
                max_results=max_results,
            )
        )

    def get_sessions(
        self,
        *,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        pod_id: Optional[str] = None,
    ) -> List[Dict]:
        """
        Get court availability sessions from Podplay API.

        Note: The /sessions endpoint does NOT support pagination (no ipp/page params).
        It returns all sessions in the date range in a single response.
        startTime must be passed as an array.

        Args:
            start_time: Start time for sessions (required, will be passed as array)
            end_time: End time for sessions (required)
            pod_id: Pod ID to filter sessions (from organizations.podplay_pod_id)
        """
        if not start_time or not end_time:
            raise ValueError("start_time and end_time are required for get_sessions")

        # startTime must be an array according to API docs
        params: Dict = {
            "startTime": [self._to_iso(start_time)],  # Array format
            "endTime": self._to_iso(end_time),
        }

        if pod_id:
            # podId is also an array according to API docs
            params["podId"] = [pod_id]

        print(
            f"[GET SESSIONS] startTime={params['startTime']} | "
            f"endTime={params['endTime']} | podId={params.get('podId')}"
        )

        # Make a single API call - no pagination for /sessions endpoint
        payload = self._request("GET", "/sessions", params=params)
        items = payload.get("items", [])

        print(f"[GET SESSIONS] Retrieved {len(items)} sessions in single API call")

        return items
