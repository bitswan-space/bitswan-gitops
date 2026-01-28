import logging
import time
from fastapi import Depends, FastAPI, Request
from fastapi.openapi.utils import get_openapi

from app.lifespan import lifespan
from app.routes.automations import router as automations_router
from app.routes.images import router as images_router
from app.routes.jupyter import router as jupyter_router
from app.routes.docs import router as docs_router
from app.dependencies import verify_token


import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

debug = os.environ.get("DEBUG", "false").lower() == "true"

# Threshold in milliseconds for slow endpoint warning
SLOW_ENDPOINT_THRESHOLD_MS = 150

app = FastAPI(lifespan=lifespan, debug=debug)


@app.middleware("http")
async def log_slow_requests(request: Request, call_next):
    """Middleware to log warnings for slow endpoints (>150ms)."""
    start_time = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start_time) * 1000

    if elapsed_ms > SLOW_ENDPOINT_THRESHOLD_MS:
        logger.warning(
            f"Slow endpoint: {request.method} {request.url.path} took {elapsed_ms:.1f}ms"
        )

    return response


# Apply auth to protected routes only
app.include_router(automations_router, dependencies=[Depends(verify_token)])
app.include_router(images_router, dependencies=[Depends(verify_token)])
app.include_router(jupyter_router, dependencies=[Depends(verify_token)])
# Docs router is public - no auth required
app.include_router(docs_router)


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title="Bitswan GitOps API",
        version="0.1.0",
        description="""
# Bitswan GitOps API

This API allows you to manage Bitswan automations, including deployment, monitoring, and promotion workflows.

## Documentation

- **[Promotion Guide](/docs/promotion-guide)** - Complete guide for the promotion workflow (dev → staging → production) and rollback procedures
        """,
        routes=app.routes,
    )

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi
