"""Production ASGI entrypoint."""
import app as base
from relevance import score_relevance

base.score_relevance = score_relevance
app = base.app
