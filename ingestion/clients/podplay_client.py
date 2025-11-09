from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

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
            print(
                f"[PodplayClient] GET {path} page={page_params.get('page')} "
                f"ipp={page_params.get('ipp')} (so far {yielded} records)"
            )
            payload = self._request("GET", path, params=page_params)
            items = payload.get("items", [])

            if not items:
                break

            for item in items:
                yield item
                yielded += 1
                if max_results and yielded >= max_results:
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

            if total_pages and page >= total_pages:
                break
            if total and ipp and page * ipp >= total:
                break
            if count is not None and ipp and count < ipp:
                break

            page += 1

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
        page_size: int = 100,
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
