import asyncio
import logging
import os
import time
from collections import defaultdict

from fastapi import Depends, FastAPI, Request
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse

from app.lifespan import lifespan
from app.routes.automations import router as automations_router
from app.routes.images import router as images_router
from app.routes.jupyter import router as jupyter_router
from app.routes.services import router as services_router
from app.routes.docs import router as docs_router
from app.routes.events import router as events_router
from app.routes.worktrees import router as worktrees_router
from app.routes.agent import router as agent_router
from app.routes.backups import router as backups_router
from app.dependencies import verify_token

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


# Endpoint spray detection: track 404s per IP
_probe_tracker: dict[str, list[float]] = defaultdict(list)
_PROBE_WINDOW = 60  # seconds
_PROBE_WARN_THRESHOLD = 10  # warn after this many 404s
_PROBE_BLOCK_THRESHOLD = 20  # block after this many 404s
_blocked_ips: dict[str, float] = {}  # ip → block_until timestamp
_BLOCK_DURATION = 300  # block for 5 minutes


@app.middleware("http")
async def security_middleware(request: Request, call_next):
    """Middleware: slow request logging + endpoint spray detection."""
    client_ip = request.client.host if request.client else "unknown"

    # Check if IP is blocked
    if client_ip in _blocked_ips:
        if time.time() < _blocked_ips[client_ip]:
            return JSONResponse(
                status_code=429,
                content={"detail": "Temporarily blocked due to suspicious activity."},
            )
        else:
            del _blocked_ips[client_ip]

    start_time = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start_time) * 1000

    if elapsed_ms > SLOW_ENDPOINT_THRESHOLD_MS:
        logger.warning(
            f"Slow endpoint: {request.method} {request.url.path} took {elapsed_ms:.1f}ms"
        )

    # Track 404s for endpoint spray detection
    if response.status_code == 404:
        now = time.time()
        hits = _probe_tracker[client_ip]
        _probe_tracker[client_ip] = [t for t in hits if now - t < _PROBE_WINDOW]
        _probe_tracker[client_ip].append(now)
        count = len(_probe_tracker[client_ip])

        if count >= _PROBE_BLOCK_THRESHOLD:
            _blocked_ips[client_ip] = now + _BLOCK_DURATION
            logger.warning(
                "SECURITY: Endpoint spray blocked — %d 404s from %s in %ds. "
                "Blocked for %ds.",
                count,
                client_ip,
                _PROBE_WINDOW,
                _BLOCK_DURATION,
            )
            _broadcast_security_warning("endpoint_spray_blocked", client_ip, count)
        elif count == _PROBE_WARN_THRESHOLD:
            logger.warning(
                "SECURITY: Possible endpoint probing — %d 404s from %s in %ds",
                count,
                client_ip,
                _PROBE_WINDOW,
            )
            _broadcast_security_warning("endpoint_spray_detected", client_ip, count)

    return response


def _broadcast_security_warning(warning_type: str, source_ip: str, count: int):
    """Broadcast a security warning via SSE to connected editors."""
    try:
        from app.event_broadcaster import event_broadcaster

        warning = {
            "type": warning_type,
            "source_ip": source_ip,
            "count": count,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(event_broadcaster.broadcast("security_warning", warning))
        except RuntimeError:
            pass
    except ImportError:
        pass


# Apply auth to protected routes only
app.include_router(automations_router, dependencies=[Depends(verify_token)])
app.include_router(images_router, dependencies=[Depends(verify_token)])
app.include_router(jupyter_router, dependencies=[Depends(verify_token)])
app.include_router(services_router, dependencies=[Depends(verify_token)])
app.include_router(events_router, dependencies=[Depends(verify_token)])
app.include_router(worktrees_router, dependencies=[Depends(verify_token)])
# Docs router is public - no auth required
app.include_router(docs_router)
# Agent router uses its own verify_agent_token dependency (per-route)
app.include_router(agent_router)
# Backups router - protected by main auth
app.include_router(backups_router, dependencies=[Depends(verify_token)])


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
