from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from institutional_auth import create_client_key, disable_client
from institutional_exports import generate_release, list_releases, release_manifest
from institutional_ops_api import admin_auth

router = APIRouter(tags=["institutional-admin"])


class ClientCreate(BaseModel):
    client_name: str = Field(min_length=1, max_length=200)
    scopes: str = Field(default="read,stream", max_length=200)


class ReleaseCreate(BaseModel):
    as_of: Optional[datetime] = None
    notes: Optional[str] = Field(default=None, max_length=4000)


@router.post("/internal/api-clients", dependencies=[Depends(admin_auth)])
def create_api_client(body: ClientCreate):
    allowed = {"read", "stream", "admin"}
    requested = {x.strip() for x in body.scopes.split(",") if x.strip()}
    if not requested or not requested <= allowed:
        raise HTTPException(400, f"Scopes must be a subset of {sorted(allowed)}")
    return create_client_key(body.client_name, ",".join(sorted(requested)))


@router.delete("/internal/api-clients/{client_id}", dependencies=[Depends(admin_auth)])
def revoke_api_client(client_id: str):
    if not disable_client(client_id):
        raise HTTPException(404, "API client not found")
    return {"client_id": client_id, "active": False}


@router.post("/internal/releases", dependencies=[Depends(admin_auth)])
def create_release(body: ReleaseCreate):
    try:
        return generate_release(body.as_of, body.notes)
    except RuntimeError as exc:
        raise HTTPException(503, str(exc))


@router.get("/v1/releases")
def releases(limit: int = Query(50, ge=1, le=500)):
    return list_releases(limit)


@router.get("/v1/releases/{release_id}")
def release(release_id: str):
    manifests = release_manifest(release_id)
    if not manifests:
        raise HTTPException(404, "Release not found")
    return {"release_id": release_id, "datasets": manifests}
