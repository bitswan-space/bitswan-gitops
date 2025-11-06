import logging
from fastapi import Depends, FastAPI
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

debug = os.environ.get("DEBUG", "false").lower() == "true"

app = FastAPI(lifespan=lifespan, debug=debug)

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
