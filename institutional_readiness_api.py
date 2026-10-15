from fastapi import APIRouter

from institutional_readiness_v5 import (
    evaluate_enrichment_readiness,
    evaluate_identity_readiness,
    evaluate_observed_readiness,
    evaluate_readiness,
)

router = APIRouter(tags=["institutional-readiness"])


@router.get("/v1/readiness")
def readiness():
    """Return layered distribution readiness; advisory enrichment does not block core readiness."""
    return evaluate_readiness()


@router.get("/v1/readiness/observed")
def observed_readiness():
    """Layer 1: source capture, raw evidence, market state and curve readiness."""
    return evaluate_observed_readiness()


@router.get("/v1/readiness/identity")
def identity_readiness():
    """Layer 2 core: deterministic canonical identity readiness only."""
    return evaluate_identity_readiness()


@router.get("/v1/readiness/enrichment")
def enrichment_readiness():
    """Layer 2 advisory: fuzzy review discovery, ontology and knowledge-graph readiness."""
    return evaluate_enrichment_readiness()
