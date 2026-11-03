import logging
from fastapi import FastAPI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("keyframe.gateway")

app = FastAPI(
    title="KeyFrame-AI API gateway",
    description="Distributed Multimodal Video Note-Taking Agent API",
    version="0.1.0",
)


@app.get("/healthz")
async def healthz():
    """Basic testing check."""
    return {"status": "ok"}