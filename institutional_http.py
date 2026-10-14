"""Thread-local resilient HTTP client for institutional source collection.

Each source-fetch worker gets its own requests.Session. This avoids sharing mutable session
state between concurrent venue fetches while retaining retry/backoff semantics.
"""

from __future__ import annotations

import threading

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_local = threading.local()


def session() -> requests.Session:
    current = getattr(_local, "session", None)
    if current is not None:
        return current
    current = requests.Session()
    current.headers.update({"User-Agent": "EventRiskInstitutional/2.3"})
    current.mount("https://", HTTPAdapter(max_retries=Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )))
    _local.session = current
    return current


def get_json(url: str, **params):
    response = session().get(
        url,
        params={k: v for k, v in params.items() if v is not None},
        timeout=40,
    )
    response.raise_for_status()
    return response.json()
