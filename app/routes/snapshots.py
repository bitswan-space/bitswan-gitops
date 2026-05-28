"""
Snapshot routes: create, list, delete, clone, estimate and task inspection.
"""

import asyncio
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.dependencies import get_snapshot_manager, get_snapshot_service
from app.services.snapshot_service import SnapshotService
from app.snapshot_manager import SnapshotManager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/snapshots", tags=["snapshots"])

_bg_tasks: set[asyncio.Task] = set()


def _spawn_bg(coro) -> asyncio.Task:
    t = asyncio.create_task(coro)
    _bg_tasks.add(t)
    t.add_done_callback(_bg_tasks.discard)
    return t


# ------------------------------------------------------------------
# Request bodies
# ------------------------------------------------------------------


class CreateSnapshotRequest(BaseModel):
    source_stage: str
    name: str | None = None
    # When set, runs the per-worktree Postgres-only path (source_stage is
    # forced to "dev"). Otherwise the stage-scoped snapshot path runs.
    worktree: str | None = None


class CloneSnapshotRequest(BaseModel):
    # Exactly one of target_stage / target_worktree must be set, depending on
    # the kind of snapshot referenced.
    target_stage: str | None = None
    target_worktree: str | None = None
    confirm_destination_is_production: bool = False


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------


@router.post("", status_code=202)
async def create_snapshot(
    body: CreateSnapshotRequest,
    service: Annotated[SnapshotService, Depends(get_snapshot_service)],
):
    """Snapshot a stage's Postgres + CouchDB + MinIO data,
    or a single worktree's Postgres DB when `worktree` is supplied."""
    try:
        task_id = await service.create_snapshot(
            source_stage=body.source_stage,
            name=body.name,
            worktree=body.worktree,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"task_id": task_id}


@router.get("")
async def list_snapshots(
    service: Annotated[SnapshotService, Depends(get_snapshot_service)],
    manager: Annotated[SnapshotManager, Depends(get_snapshot_manager)],
):
    """List all snapshots and retention setting."""
    return {
        "snapshots": service.list_snapshots(),
        "retention_per_stage": service.retention_per_stage,
        "tasks": manager.to_dict_all(),
    }


@router.get("/estimate")
async def estimate_size(
    stage: Annotated[str, Query()],
    service: Annotated[SnapshotService, Depends(get_snapshot_service)],
    worktree: Annotated[str | None, Query()] = None,
):
    """Synchronous size estimate. With `worktree`, returns just the per-worktree
    Postgres DB size; otherwise the full stage (postgres + couchdb + minio)."""
    try:
        return await service.estimate_size(stage, worktree=worktree)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/tasks")
async def list_tasks(
    manager: Annotated[SnapshotManager, Depends(get_snapshot_manager)],
):
    """Debug: list all in-memory snapshot tasks."""
    return manager.to_dict_all()


@router.get("/{snapshot_id}")
async def get_snapshot(
    snapshot_id: str,
    service: Annotated[SnapshotService, Depends(get_snapshot_service)],
):
    """Return the manifest for a single snapshot."""
    snapshots = service.list_snapshots()
    for snap in snapshots:
        if snap.get("snapshot_id") == snapshot_id:
            return snap
    raise HTTPException(status_code=404, detail=f"Snapshot '{snapshot_id}' not found")


@router.delete("/{snapshot_id}", status_code=202)
async def delete_snapshot(
    snapshot_id: str,
    service: Annotated[SnapshotService, Depends(get_snapshot_service)],
):
    """Delete a snapshot from disk."""
    snapshots = service.list_snapshots()
    ids = {s.get("snapshot_id") for s in snapshots}
    if snapshot_id not in ids:
        raise HTTPException(
            status_code=404, detail=f"Snapshot '{snapshot_id}' not found"
        )
    task_id = await service.delete_snapshot(snapshot_id)
    return {"task_id": task_id}


@router.post("/{snapshot_id}/clone", status_code=202)
async def clone_snapshot(
    snapshot_id: str,
    body: CloneSnapshotRequest,
    service: Annotated[SnapshotService, Depends(get_snapshot_service)],
):
    """Clone a snapshot into a target stage, or into a target worktree
    when the snapshot is worktree-kind."""
    if bool(body.target_stage) == bool(body.target_worktree):
        raise HTTPException(
            status_code=400,
            detail="Exactly one of target_stage or target_worktree must be set",
        )
    try:
        task_id = await service.clone_snapshot(
            snapshot_id=snapshot_id,
            target_stage=body.target_stage,
            target_worktree=body.target_worktree,
            confirm_production=body.confirm_destination_is_production,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        # 409 for "target busy" or similar concurrency conflicts
        if "busy" in str(exc).lower() or "active deploy" in str(exc).lower():
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"task_id": task_id}


@router.post("/tasks/{task_id}/resume-target", status_code=202)
async def resume_target(
    task_id: str,
    service: Annotated[SnapshotService, Depends(get_snapshot_service)],
    manager: Annotated[SnapshotManager, Depends(get_snapshot_manager)],
):
    """Restart the target's automations after a partial-failure clone.
    Dispatches to worktree or stage scope based on the task fields."""
    task = manager.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found")
    if task.target_worktree is None and task.target_stage is None:
        raise HTTPException(
            status_code=400, detail="Task has no target stage or worktree"
        )
    _spawn_bg(
        service.resume_target_automations(
            task_id,
            target_stage=task.target_stage,
            target_worktree=task.target_worktree,
        )
    )
    return {"task_id": task_id}
