import os
from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from app.services.automation_service import AutomationService


def verify_token(credentials: HTTPAuthorizationCredentials = Security(HTTPBearer())):
    secret_token = os.environ.get("BITSWAN_GITOPS_SECRET")
    if credentials.scheme != "Bearer" or credentials.credentials != secret_token:
        raise HTTPException(
            status_code=401,
            detail="Unauthorized: Invalid or missing token",
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_automation_service():
    return AutomationService()
