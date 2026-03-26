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

    backup_service.save_backup_config(config)

    # Try to discover existing setup on S3
    recovered = False
    if not backup_service.get_restic_key():
        # Check if there's a key on S3 (existing backups from another server)
        if await backup_service.key_exists_on_s3(config):
            key = await backup_service.download_key_from_s3(config)
            if key:
                backup_service._save_key(key)
                recovered = True

    # Generate new key if we still don't have one
    if not backup_service.get_restic_key():
        backup_service.generate_restic_key()

    # Initialize the restic repo (idempotent — succeeds if already initialized)
    ok, msg = await backup_service.init_repo(config)
    if not ok:
        raise HTTPException(
            status_code=500, detail=f"Failed to initialize restic repo: {msg}"
        )

    # Upload key to S3 as a convenience backup (if we generated a new one)
    if not recovered:
        key = backup_service.get_restic_key()
        if key:
            await backup_service.upload_key_to_s3(config, key)

    result = {"status": "configured", "message": msg}
    if recovered:
        result["recovered"] = True
        result["message"] = (
            "Connected to existing backup repository. Key recovered from S3."
        )
    return result


# --- Key management ---
# The encryption key always lives on the local server (secrets/.backup/restic-key).
# On setup, it's also uploaded to S3 as a convenience backup.
# The user can download it (to store in a password manager) and delete it from S3
# so that a compromised S3 store can't be used to decrypt backups.


@router.get("/key")
async def get_key():
    """Download the restic encryption key for offline storage."""
    key = backup_service.get_restic_key()
    if not key:
        raise HTTPException(status_code=404, detail="No encryption key found")
    return {"key": key}


@router.get("/key/s3-status")
async def key_s3_status():
    """Check if the key exists on S3."""
    config = backup_service.get_backup_config()
    if not config:
        raise HTTPException(status_code=400, detail="Backup not configured")
    exists = await backup_service.key_exists_on_s3(config)
    return {"on_s3": exists}


@router.post("/key/upload-to-s3")
async def upload_key_to_s3():
    """Upload the encryption key to S3 as a backup copy."""
    config = backup_service.get_backup_config()
    key = backup_service.get_restic_key()
    if not config or not key:
        raise HTTPException(status_code=400, detail="Backup not configured or no key")
    ok, msg = await backup_service.upload_key_to_s3(config, key)
    if not ok:
        raise HTTPException(status_code=500, detail=msg)
    return {"status": "uploaded", "message": "Key uploaded to S3"}


@router.delete("/key/s3")
async def delete_key_from_s3():
    """Delete the encryption key from S3. The local copy remains.
    WARNING: If the local server is lost and you haven't downloaded the key,
    all backups become unrecoverable."""
    config = backup_service.get_backup_config()
    if not config:
        raise HTTPException(status_code=400, detail="Backup not configured")
    ok, msg = await backup_service.delete_key_from_s3(config)
    if not ok:
        raise HTTPException(status_code=500, detail=msg)
    return {
        "status": "deleted",
        "message": "Key deleted from S3. Local copy still exists for making backups.",
    }


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
