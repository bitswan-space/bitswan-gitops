import hmac
import logging
import os
import time
from collections import defaultdict

from fastapi import HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from app.services.automation_service import AutomationService
from app.services.image_service import ImageService
from app.services.jupyter_service import JupyterService

logger = logging.getLogger(__name__)

# Rate limiting: track failed auth attempts per IP
_auth_failures: dict[str, list[float]] = defaultdict(list)
_AUTH_WINDOW = 60  # seconds
_AUTH_MAX_FAILURES = 10  # max failures per window
_AUTH_WARN_THRESHOLD = 5  # warn after this many failures


def _check_rate_limit(client_ip: str):
    """Block IP if too many auth failures in the window. Warn on suspicious activity."""
    now = time.time()
    failures = _auth_failures[client_ip]
    # Prune old entries
    _auth_failures[client_ip] = [t for t in failures if now - t < _AUTH_WINDOW]
    count = len(_auth_failures[client_ip])
    if count >= _AUTH_MAX_FAILURES:
        logger.warning(
            "SECURITY: Brute force blocked — %d auth failures from %s in %ds",
            count,
            client_ip,
            _AUTH_WINDOW,
        )
        raise HTTPException(
            status_code=429,
            detail="Too many authentication failures. Try again later.",
        )
    if count == _AUTH_WARN_THRESHOLD:
        logger.warning(
            "SECURITY: Possible brute force — %d auth failures from %s in %ds",
            count,
            client_ip,
            _AUTH_WINDOW,
        )
        # Broadcast security warning via SSE so the editor can show it
        try:
            from app.event_broadcaster import event_broadcaster
            import asyncio

            warning = {
                "type": "brute_force_attempt",
                "source_ip": client_ip,
                "failures": count,
                "window_seconds": _AUTH_WINDOW,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(
                    event_broadcaster.broadcast("security_warning", warning)
                )
            except RuntimeError:
                pass  # No event loop — skip SSE broadcast
        except ImportError:
            pass


def verify_token(
    credentials: HTTPAuthorizationCredentials = Security(HTTPBearer()),
    request: Request = None,
):
    client_ip = request.client.host if request and request.client else "unknown"
    _check_rate_limit(client_ip)

    secret_token = os.environ.get("BITSWAN_GITOPS_SECRET")
    if credentials.scheme != "Bearer" or not hmac.compare_digest(
        credentials.credentials, secret_token or ""
    ):
        _auth_failures[client_ip].append(time.time())
        raise HTTPException(
            status_code=401,
            detail="Unauthorized: Invalid or missing token",
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_image_service():
    return ImageService()


_automation_service = AutomationService()


def get_automation_service():
    return _automation_service


def get_jupyter_service():
    return JupyterService()
