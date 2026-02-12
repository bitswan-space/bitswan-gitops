"""
FastAPI routes for infrastructure service management (CouchDB, Kafka).

Mirrors the automation server's HTTP API for service management,
allowing the daemon to proxy requests here.
"""

import os

from fastapi import APIRouter, HTTPException

from app.models import (
    ServiceActionRequest,
    ServiceBackupRequest,
    ServiceDisableRequest,
    ServiceEnableRequest,
    ServiceRestoreRequest,
)
from app.services.infra_service import get_service

router = APIRouter(prefix="/services", tags=["services"])

SUPPORTED_SERVICES = ("couchdb", "kafka")


def _get_workspace_name() -> str:
    return os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace-local")


def _validate_service_type(service_type: str) -> None:
    if service_type not in SUPPORTED_SERVICES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown service type: {service_type}. Supported: {', '.join(SUPPORTED_SERVICES)}",
        )


@router.post("/{service_type}/enable")
async def enable_service(service_type: str, request: ServiceEnableRequest):
    """Enable an infrastructure service."""
    _validate_service_type(service_type)
    workspace = _get_workspace_name()

    try:
        svc = get_service(
            service_type,
            workspace,
            stage=request.stage,
            image=request.image,
            kafka_image=request.kafka_image,
            ui_image=request.ui_image,
        )
        result = await svc.enable()
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{service_type}/disable")
async def disable_service(service_type: str, request: ServiceDisableRequest):
    """Disable an infrastructure service."""
    _validate_service_type(service_type)
    workspace = _get_workspace_name()

    try:
        svc = get_service(service_type, workspace, stage=request.stage)
        result = await svc.disable()
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{service_type}/status")
async def get_service_status(
    service_type: str,
    stage: str = "",
    show_passwords: bool = False,
):
    """Get the status of an infrastructure service."""
    _validate_service_type(service_type)
    workspace = _get_workspace_name()

    try:
        svc = get_service(service_type, workspace, stage=stage)
        result = await svc.status(show_passwords=show_passwords)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{service_type}/start")
async def start_service(service_type: str, request: ServiceActionRequest):
    """Start an infrastructure service."""
    _validate_service_type(service_type)
    workspace = _get_workspace_name()

    try:
        svc = get_service(service_type, workspace, stage=request.stage)
        result = await svc.start()
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{service_type}/stop")
async def stop_service(service_type: str, request: ServiceActionRequest):
    """Stop an infrastructure service."""
    _validate_service_type(service_type)
    workspace = _get_workspace_name()

    try:
        svc = get_service(service_type, workspace, stage=request.stage)
        result = await svc.stop()
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{service_type}/update")
async def update_service(service_type: str, request: ServiceActionRequest):
    """Update an infrastructure service (restart to pick up new compose config)."""
    _validate_service_type(service_type)
    workspace = _get_workspace_name()

    try:
        svc = get_service(service_type, workspace, stage=request.stage)
        if not svc.is_enabled():
            raise ValueError(f"{svc.display_name} is not enabled")
        await svc.stop()
        await svc.start()
        return {"status": "updated", "service": service_type}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/couchdb/backup")
async def backup_couchdb(request: ServiceBackupRequest):
    """Backup CouchDB databases to a tarball."""
    workspace = _get_workspace_name()

    try:
        from app.services.couchdb_service import CouchDBService

        svc = CouchDBService(workspace, stage=request.stage)
        result = await svc.backup(backup_path=request.backup_path)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/couchdb/restore")
async def restore_couchdb(request: ServiceRestoreRequest):
    """Restore CouchDB databases from a backup."""
    workspace = _get_workspace_name()

    try:
        from app.services.couchdb_service import CouchDBService

        svc = CouchDBService(workspace, stage=request.stage)
        result = await svc.restore(
            backup_path=request.backup_path, force=request.force
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
