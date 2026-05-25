import os
from pathlib import Path

from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.services.automation_service import AutomationService
from app.services.image_service import ImageService
from app.services.jupyter_service import JupyterService
from app.snapshot_manager import SnapshotManager, snapshot_manager as _snapshot_manager


def verify_token(credentials: HTTPAuthorizationCredentials = Security(HTTPBearer())):
    secret_token = os.environ.get("BITSWAN_GITOPS_SECRET")
    if credentials.scheme != "Bearer" or credentials.credentials != secret_token:
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


def get_snapshot_manager() -> SnapshotManager:
    return _snapshot_manager


def get_snapshot_service():
    from app.services.snapshot_service import SnapshotService

    workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace-local")
    bs_home = os.environ.get("BITSWAN_GITOPS_DIR", "/mnt/repo/pipeline")
    default_snap_dir = os.path.join(bs_home, "snapshots")
    snapshots_dir = Path(os.environ.get("SNAPSHOT_DIR", default_snap_dir))
    retention = int(os.environ.get("SNAPSHOT_RETENTION_PER_STAGE", "5"))
    return SnapshotService(
        workspace_name=workspace_name,
        snapshot_manager=_snapshot_manager,
        snapshots_dir=snapshots_dir,
        retention_per_stage=retention,
    )
