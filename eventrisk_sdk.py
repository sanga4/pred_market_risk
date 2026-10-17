"""Python SDK for the EventRisk institutional data API.

The SDK exposes observed/normalized data primitives rather than portfolio recommendations.
Point-in-time-safe methods default to the mapping EventRisk actually knew when data arrived.
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
        self.session.headers.update({"User-Agent": "eventrisk-python/2.2"})
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

    def readiness(self):
        return self._get("/v1/readiness")

    def sources(self):
        return self._get("/v1/sources")

    def explorer_summary(self, *, source=None):
        return self._get("/v1/explorer/summary", {"source": source})

    def explorer_events(self, *, q=None, theme=None, subject_id=None, source=None, after=None, limit=30):
        return self._get("/v1/explorer/events", {
            "q": q, "theme": theme, "subject_id": subject_id, "source": source,
            "after": after, "limit": limit,
        })

    def explorer_subjects(self, *, q=None, theme=None, source=None, after=None, limit=30):
        """Discover persisted semantic topics and their governed themes."""
        return self._get("/v1/explorer/subjects", {
            "q": q, "theme": theme, "source": source, "after": after, "limit": limit,
        })

    def event_mappings(self, event_id: str, *, include_expired=False):
        return self._get(f"/v1/explorer/events/{event_id}/mappings", {
            "include_expired": str(include_expired).lower(),
        })

    def hierarchy_metrics(self):
        return self._get("/v1/hierarchy/metrics")

    def hierarchy_taxonomy(self, *, version=None):
        return self._get("/v1/hierarchy/taxonomy", {"version": version})

    def event_hierarchy(self, event_id: str, *, at=None):
        return self._get(f"/v1/hierarchy/events/{event_id}", {"at": at})

    def hierarchy_review_queue(self, *, after=None, limit=50, abstained_only=True):
        return self._get("/v1/hierarchy/review", {"after": after, "limit": limit, "abstained_only": str(abstained_only).lower()})

    def events(self, *, family_id=None, status="active", category=None, include_unserved=False, limit=100, offset=0):
        return self._get("/v1/events", {
            "family_id": family_id,
            "status": status,
            "category": category,
            "include_unserved": str(include_unserved).lower(),
            "limit": limit,
            "offset": offset,
        })

    def event(self, event_id: str):
        return self._get(f"/v1/events/{event_id}")

    def event_curve(self, event_id: str, *, mapping_basis="as_known", source_id=None,
                    start=None, end=None, as_of=None, limit=25000):
        return self._get(f"/v1/events/{event_id}/curve", {
            "mapping_basis": mapping_basis, "source_id": source_id,
            "start": self._time(start), "end": self._time(end),
            "as_of": self._time(as_of), "limit": limit,
        })

    def event_curve_latest(self, event_id: str, *, mapping_basis="as_known", at=None):
        return self._get(f"/v1/events/{event_id}/curve/latest", {
            "mapping_basis": mapping_basis, "at": self._time(at),
        })

    def event_history(self, event_id: str, *, source_id=None, start=None, end=None, limit=10000):
        """History restated through the event's current active mapping."""
        return self._get(f"/v1/events/{event_id}/history", {
            "source_id": source_id, "start": self._time(start), "end": self._time(end), "limit": limit,
        })

    def event_history_point_in_time(self, event_id: str, *, source_id=None, start=None, end=None, limit=10000):
        return self._get(f"/v1/events/{event_id}/history-point-in-time", {
            "source_id": source_id, "start": self._time(start), "end": self._time(end), "limit": limit,
        })

    def event_as_of(self, event_id: str, at: datetime | str):
        return self._get(f"/v1/events/{event_id}/as-of", {"at": self._time(at)})

    def event_as_known(self, event_id: str, at: datetime | str):
        return self._get(f"/v1/events/{event_id}/as-known", {"at": self._time(at)})

    def event_versions(self, event_id: str):
        return self._get(f"/v1/events/{event_id}/versions")

    def event_family_versions(self, family_id: str):
        return self._get(f"/v1/event-families/{family_id}/versions")

    def source_comparison(self, event_id: str):
        return self._get(f"/v1/events/{event_id}/source-comparison")

    def term_structure(self, family_id: str, as_of=None):
        return self._get(f"/v1/event-families/{family_id}/term-structure", {"as_of": self._time(as_of)})

    def event_relationships(self, event_id: str, *, include_inferred=False, include_expired=False):
        return self._get(f"/v1/events/{event_id}/relationships", {
            "include_inferred": str(include_inferred).lower(),
            "include_expired": str(include_expired).lower(),
        })

    def entity(self, entity_id: str):
        return self._get(f"/v1/entities/{entity_id}")

    def entity_relationships(self, entity_id: str, *, include_inferred=False, include_expired=False):
        return self._get(f"/v1/entities/{entity_id}/relationships", {
            "include_inferred": str(include_inferred).lower(),
            "include_expired": str(include_expired).lower(),
        })

    def instruments(self, *, ticker=None, sector=None, country=None, active=True, after=None, limit=100):
        """List source-described institutional reference instruments."""
        return self._get("/v1/instruments", {
            "ticker": ticker,
            "sector": sector,
            "country": country,
            "active": str(active).lower(),
            "after": after,
            "limit": limit,
        })

    def instrument(self, instrument_id: str):
        return self._get(f"/v1/instruments/{instrument_id}")

    def instrument_as_of(self, instrument_id: str, at: datetime | str):
        return self._get(f"/v1/instruments/{instrument_id}/as-of", {"at": self._time(at)})

    def graph(self, node_id: str, *, include_inferred=False, limit=500):
        return self._get(f"/v1/graph/{node_id}", {
            "include_inferred": str(include_inferred).lower(), "limit": limit,
        })

    def changes(self, since: datetime | str, *, source_id=None, limit=1000):
        """Legacy timestamp convenience endpoint. Prefer change_feed for lossless pagination."""
        return self._get("/v1/changes", {
            "since": self._time(since), "source_id": source_id, "limit": limit,
        })

    def change_feed(self, *, cursor=None, since=None, source_id=None,
                    mapping_basis="as_known", limit=1000):
        """Lossless resumable observation feed. Reuse the returned `cursor` on the next call."""
        return self._get("/v1/change-feed", {
            "cursor": cursor,
            "since": self._time(since),
            "source_id": source_id,
            "mapping_basis": mapping_basis,
            "limit": limit,
        })

    def quality(self, *, severity=None, limit=250):
        return self._get("/v1/quality", {"severity": severity, "limit": limit})

    def releases(self, *, limit=50):
        return self._get("/v1/releases", {"limit": limit})

    def release_manifest(self, release_id: str):
        return self._get(f"/v1/releases/{release_id}")
