"""Minimal Python SDK for the EventRisk institutional API.

This intentionally exposes data primitives rather than portfolio recommendations.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

import requests


class EventRiskError(RuntimeError):
    pass


class EventRisk:
    def __init__(self, base_url: str, api_key: Optional[str] = None, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "eventrisk-python/1.0"})
        if api_key:
            self.session.headers["Authorization"] = f"Bearer {api_key}"

    @staticmethod
    def _time(value: Optional[datetime | str]):
        return value.isoformat() if isinstance(value, datetime) else value

    def _get(self, path: str, params: Optional[dict[str, Any]] = None):
        clean = {k: v for k, v in (params or {}).items() if v is not None}
        response = self.session.get(f"{self.base_url}{path}", params=clean, timeout=self.timeout)
        if not response.ok:
            raise EventRiskError(f"{response.status_code}: {response.text[:1000]}")
        return response.json()

    def health(self):
        return self._get("/health")

    def sources(self):
        return self._get("/v1/sources")

    def events(self, *, family_id=None, status="active", category=None, limit=100, offset=0):
        return self._get("/v1/events", {
            "family_id": family_id,
            "status": status,
            "category": category,
            "limit": limit,
            "offset": offset,
        })

    def event(self, event_id: str):
        return self._get(f"/v1/events/{event_id}")

    def event_history(
        self,
        event_id: str,
        *,
        source_id: Optional[str] = None,
        start: Optional[datetime | str] = None,
        end: Optional[datetime | str] = None,
        limit: int = 10000,
    ):
        """History restated through the event's current active mapping."""
        return self._get(f"/v1/events/{event_id}/history", {
            "source_id": source_id,
            "start": self._time(start),
            "end": self._time(end),
            "limit": limit,
        })

    def event_history_point_in_time(
        self,
        event_id: str,
        *,
        source_id: Optional[str] = None,
        start: Optional[datetime | str] = None,
        end: Optional[datetime | str] = None,
        limit: int = 10000,
    ):
        """History using the canonical mapping that was valid when EventRisk received each row."""
        return self._get(f"/v1/events/{event_id}/history-point-in-time", {
            "source_id": source_id,
            "start": self._time(start),
            "end": self._time(end),
            "limit": limit,
        })

    def event_as_of(self, event_id: str, at: datetime | str):
        """Latest source observations by cutoff, using the endpoint's restated event identity."""
        return self._get(f"/v1/events/{event_id}/as-of", {"at": self._time(at)})

    def event_as_known(self, event_id: str, at: datetime | str):
        """Canonical metadata and source observations exactly as EventRisk knew them at `at`."""
        return self._get(f"/v1/events/{event_id}/as-known", {"at": self._time(at)})

    def event_versions(self, event_id: str):
        return self._get(f"/v1/events/{event_id}/versions")

    def event_family_versions(self, family_id: str):
        return self._get(f"/v1/event-families/{family_id}/versions")

    def source_comparison(self, event_id: str):
        return self._get(f"/v1/events/{event_id}/source-comparison")

    def term_structure(self, family_id: str, as_of: Optional[datetime | str] = None):
        return self._get(
            f"/v1/event-families/{family_id}/term-structure",
            {"as_of": self._time(as_of)},
        )

    def event_relationships(self, event_id: str, *, include_inferred: bool = False):
        return self._get(
            f"/v1/events/{event_id}/relationships",
            {"include_inferred": str(include_inferred).lower()},
        )

    def entity(self, entity_id: str):
        return self._get(f"/v1/entities/{entity_id}")

    def entity_relationships(self, entity_id: str, *, include_inferred: bool = False):
        return self._get(
            f"/v1/entities/{entity_id}/relationships",
            {"include_inferred": str(include_inferred).lower()},
        )

    def graph(self, node_id: str, *, include_inferred: bool = False, limit: int = 500):
        return self._get(
            f"/v1/graph/{node_id}",
            {"include_inferred": str(include_inferred).lower(), "limit": limit},
        )

    def changes(self, since: datetime | str, *, source_id: Optional[str] = None, limit: int = 1000):
        return self._get("/v1/changes", {
            "since": self._time(since),
            "source_id": source_id,
            "limit": limit,
        })

    def quality(self, *, severity: Optional[str] = None, limit: int = 250):
        return self._get("/v1/quality", {"severity": severity, "limit": limit})

    def releases(self, *, limit: int = 50):
        return self._get("/v1/releases", {"limit": limit})

    def release_manifest(self, release_id: str):
        return self._get(f"/v1/releases/{release_id}")
