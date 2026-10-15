"""Institutional API key authentication.

Customer secrets are never stored in plaintext. The admin creation flow returns a new
key once and stores only its SHA-256 digest in er_api_clients.

The public API can authenticate in a strictly read-only mode. Usage timestamps are optional
telemetry and must never turn the public query plane into a write dependency.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import uuid
from typing import Optional

from db import connect

KEY_PREFIX = "er_live_"


def hash_key(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def extract_bearer(authorization: Optional[str], x_api_key: Optional[str]) -> Optional[str]:
    if x_api_key:
        return x_api_key.strip()
    if authorization and authorization.lower().startswith("bearer "):
        return authorization.split(" ", 1)[1].strip()
    return None


def authenticate_key(
    secret: Optional[str],
    required_scope: str = "read",
    *,
    record_usage: bool = True,
) -> Optional[dict]:
    """Return authenticated client metadata, or None.

    EVENTRISK_API_KEY remains a bootstrap/staging master key. Production customer keys are
    hashed records in er_api_clients and can be disabled independently.

    ``record_usage=False`` is the public/read-plane mode: authentication performs no database
    mutation. This lets the public service use a read-only MotherDuck credential and keeps request
    authorization independent from write availability. A separate audit/metrics path can record
    usage without weakening that boundary.
    """
    if not secret:
        return None
    bootstrap = os.getenv("EVENTRISK_API_KEY")
    if bootstrap and hmac.compare_digest(secret, bootstrap):
        return {"client_id": "bootstrap", "client_name": "bootstrap", "scopes": "read,stream"}

    digest = hash_key(secret)
    con = connect(read_only=not record_usage)
    try:
        row = con.execute(
            "SELECT client_id,client_name,scopes,active FROM er_api_clients WHERE key_hash=?",
            [digest],
        ).fetchone()
        if not row or not row[3]:
            return None
        scopes = {x.strip() for x in (row[2] or "read").split(",") if x.strip()}
        if required_scope not in scopes and "admin" not in scopes:
            return None
        if record_usage:
            con.execute("UPDATE er_api_clients SET last_used_at=now() WHERE client_id=?", [row[0]])
        return {"client_id": row[0], "client_name": row[1], "scopes": ",".join(sorted(scopes))}
    finally:
        con.close()


def create_client_key(client_name: str, scopes: str = "read,stream") -> dict:
    secret = KEY_PREFIX + secrets.token_urlsafe(32)
    client_id = f"client_{uuid.uuid4().hex}"
    con = connect()
    try:
        con.execute(
            """INSERT INTO er_api_clients(client_id,client_name,key_hash,active,scopes)
               VALUES (?,?,?,?,?)""",
            [client_id, client_name, hash_key(secret), True, scopes],
        )
        return {
            "client_id": client_id,
            "client_name": client_name,
            "scopes": scopes,
            "api_key": secret,
            "warning": "This key is returned once. Store it securely; EventRisk stores only its hash.",
        }
    finally:
        con.close()


def disable_client(client_id: str) -> bool:
    con = connect()
    try:
        before = con.execute("SELECT active FROM er_api_clients WHERE client_id=?", [client_id]).fetchone()
        if not before:
            return False
        con.execute("UPDATE er_api_clients SET active=FALSE WHERE client_id=?", [client_id])
        return True
    finally:
        con.close()
