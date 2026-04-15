import hmac
import os
from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from app.services.automation_service import AutomationService
from app.services.image_service import ImageService
from app.services.jupyter_service import JupyterService


def verify_token(credentials: HTTPAuthorizationCredentials = Security(HTTPBearer())):
    secret_token = os.environ.get("BITSWAN_GITOPS_SECRET")
    if credentials.scheme != "Bearer" or not hmac.compare_digest(
        credentials.credentials, secret_token or ""
    ):
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
