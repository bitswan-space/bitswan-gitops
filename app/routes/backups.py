"""API routes for backup management."""

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services import backup_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/backups", tags=["backups"])


# --- Configuration ---


class S3ConfigRequest(BaseModel):
    s3_endpoint: str
    s3_bucket: str
    s3_access_key: str
    s3_secret_key: str
    retention_daily: int = 30
    retention_monthly: int = 12


@router.get("/config")
async def get_config():
    """Get current backup configuration (without secret key)."""
    config = backup_service.get_backup_config()
    if not config:
        return {"configured": False}
    # Don't expose the S3 secret key
    safe = {k: v for k, v in config.items() if k != "s3_secret_key"}
    safe["configured"] = True
    safe["has_key"] = backup_service.get_restic_key() is not None
    return safe


@router.post("/config")
async def save_config(body: S3ConfigRequest):
    """Save S3 backup configuration and initialize the restic repository."""
    config = {
        "s3_endpoint": body.s3_endpoint,
        "s3_bucket": body.s3_bucket,
        "s3_access_key": body.s3_access_key,
        "s3_secret_key": body.s3_secret_key,
        "retention": {
            "daily": body.retention_daily,
            "monthly": body.retention_monthly,
        },
    }

    # Generate encryption key if none exists
    if not backup_service.get_restic_key():
        backup_service.generate_restic_key()

    backup_service.save_backup_config(config)

    # Initialize the restic repo
    ok, msg = await backup_service.init_repo(config)
    if not ok:
        raise HTTPException(
            status_code=500, detail=f"Failed to initialize restic repo: {msg}"
        )

    return {"status": "configured", "message": msg}


# --- Key management ---


@router.get("/key")
async def get_key():
    """Download the restic encryption key. Store it safely!"""
    key = backup_service.get_restic_key()
    if not key:
        raise HTTPException(status_code=404, detail="No encryption key found")
    return {"key": key}


@router.delete("/key")
async def delete_key():
    """Delete the local encryption key. Make sure you've downloaded it first!"""
    if not backup_service.get_restic_key():
        raise HTTPException(status_code=404, detail="No encryption key to delete")
    backup_service.delete_restic_key()
    return {
        "status": "deleted",
        "message": "Key deleted from server. Ensure you have a backup of the key.",
    }


@router.post("/key/restore")
async def restore_key(body: dict):
    """Restore the encryption key (e.g., after deletion for manual backup runs)."""
    key = body.get("key", "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="Key is required")
    import os

    backup_dir = backup_service._get_backup_dir()
    os.makedirs(backup_dir, mode=0o700, exist_ok=True)
    key_path = backup_service._get_key_path()
    with open(key_path, "w") as f:
        f.write(key)
    os.chmod(key_path, 0o600)
    return {"status": "restored"}


# --- Backup operations ---


@router.post("/run")
async def run_backup_now():
    """Trigger an immediate backup of all production data."""
    config = backup_service.get_backup_config()
    if not config:
        raise HTTPException(status_code=400, detail="Backup not configured")
    if not backup_service.get_restic_key():
        raise HTTPException(status_code=400, detail="No encryption key")

    try:
        results = await backup_service.run_backup(config)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/snapshots")
async def list_snapshots(tag: str = None):
    """List available backup snapshots."""
    config = backup_service.get_backup_config()
    if not config:
        raise HTTPException(status_code=400, detail="Backup not configured")
    if not backup_service.get_restic_key():
        raise HTTPException(status_code=400, detail="No encryption key")

    snapshots = await backup_service.list_snapshots(config, tag=tag)
    return {"snapshots": snapshots}


# --- Restore operations ---


class RestoreRequest(BaseModel):
    snapshot_id: str
    stage: str = "production"


@router.post("/restore/postgres")
async def restore_postgres(body: RestoreRequest):
    """Restore a Postgres backup to a given stage."""
    config = backup_service.get_backup_config()
    if not config or not backup_service.get_restic_key():
        raise HTTPException(status_code=400, detail="Backup not configured or no key")

    ok, msg = await backup_service.restore_postgres(
        config, body.snapshot_id, body.stage
    )
    if not ok:
        raise HTTPException(status_code=500, detail=msg)
    return {"status": "restored", "message": msg}


@router.post("/restore/couchdb")
async def restore_couchdb(body: RestoreRequest):
    """Restore a CouchDB backup to a given stage."""
    config = backup_service.get_backup_config()
    if not config or not backup_service.get_restic_key():
        raise HTTPException(status_code=400, detail="Backup not configured or no key")

    ok, msg = await backup_service.restore_couchdb(config, body.snapshot_id, body.stage)
    if not ok:
        raise HTTPException(status_code=500, detail=msg)
    return {"status": "restored", "message": msg}


class WorkspaceRestoreRequest(BaseModel):
    snapshot_id: str


@router.post("/restore/workspace")
async def restore_workspace(body: WorkspaceRestoreRequest):
    """Restore workspace files to /tmp/restores/{datetime}."""
    config = backup_service.get_backup_config()
    if not config or not backup_service.get_restic_key():
        raise HTTPException(status_code=400, detail="Backup not configured or no key")

    ok, msg = await backup_service.restore_workspace(config, body.snapshot_id)
    if not ok:
        raise HTTPException(status_code=500, detail=msg)
    return {"status": "restored", "message": msg}
