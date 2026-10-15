"""Production ASGI entrypoint."""
import app as base
from institutional_api import router as institutional_router
from relevance import score_relevance

base.score_relevance = score_relevance
base.app.include_router(institutional_router)
app = base.app
