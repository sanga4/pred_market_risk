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

    def _get(self, path: str, params: Optional[dict[str, Any]] = None):
        response = self.session.get(f"{self.base_url}{path}", params=params, timeout=self.timeout)
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
        return self._get(f"/v1/events/{event_id}/history", {
            "source_id": source_id,
            "start": start.isoformat() if isinstance(start, datetime) else start,
            "end": end.isoformat() if isinstance(end, datetime) else end,
            "limit": limit,
        })

    def event_as_of(self, event_id: str, at: datetime | str):
        return self._get(
            f"/v1/events/{event_id}/as-of",
            {"at": at.isoformat() if isinstance(at, datetime) else at},
        )

    def source_comparison(self, event_id: str):
        return self._get(f"/v1/events/{event_id}/source-comparison")

    def term_structure(self, family_id: str, as_of: Optional[datetime | str] = None):
        value = as_of.isoformat() if isinstance(as_of, datetime) else as_of
        return self._get(f"/v1/event-families/{family_id}/term-structure", {"as_of": value})

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
            "since": since.isoformat() if isinstance(since, datetime) else since,
            "source_id": source_id,
            "limit": limit,
        })

    def quality(self, *, severity: Optional[str] = None, limit: int = 250):
        return self._get("/v1/quality", {"severity": severity, "limit": limit})

    def releases(self, *, limit: int = 50):
        return self._get("/v1/releases", {"limit": limit})

    def release_manifest(self, release_id: str):
        return self._get(f"/v1/releases/{release_id}")
