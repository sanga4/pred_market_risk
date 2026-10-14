from fastapi import APIRouter

from institutional_readiness_v2 import evaluate_readiness

router = APIRouter(tags=["institutional-readiness"])


@router.get("/v1/readiness")
def readiness():
    """Return strict data-distribution readiness controls, separate from API liveness."""
    return evaluate_readiness()
