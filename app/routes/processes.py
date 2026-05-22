"""
REST surface over the workspace's business processes.

Reads the in-memory cache maintained by `ProcessService`; the cache is kept
fresh by the workspace + worktree file-system watchers in `lifespan.py`.
Same data is broadcast over `/events/stream` as a `processes` event for
push-style consumers (the workspace dashboard).
"""

import re

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.event_broadcaster import event_broadcaster
from app.mqtt_processes import process_service

router = APIRouter(prefix="/processes", tags=["processes"])

# Mirrors the dashboard-side validation, kept tight enough that the name can
# also stand in for a deployment_id segment without surprises.
_PROCESS_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
# Matches the canonical worktree-name constraint used by /worktrees and /templates.
_WORKTREE_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9\-]*$")


class CreateProcessRequest(BaseModel):
    name: str
    worktree: str | None = None


@router.get("/")
async def get_processes() -> list[dict]:
    """Return all business processes across main repo and every worktree.

    Each entry is deduplicated by directory name; the `worktrees` list says
    which worktrees the same BP also lives in (empty when only in main).
    A BP that exists *only* in a worktree comes back with `in_main: false`.
    """
    return process_service.get_all_processes()


@router.post("/")
async def create_process(body: CreateProcessRequest) -> dict:
    """Create a new business-process directory.

    Scaffolds `process.toml` + `README.md` under either `<workspace_repo>/<name>/`
    or `<workspace_repo>/worktrees/<worktree>/<name>/`. The new BP surfaces
    over the SSE feed within the same response (this route refreshes the
    cache + broadcasts inline instead of waiting for the filesystem watcher
    to debounce).
    """
    name = (body.name or "").strip()
    if not _PROCESS_NAME_RE.match(name):
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid process name: must start with a letter or digit and "
                "contain only letters, digits, underscores, dashes, and dots."
            ),
        )
    if body.worktree is not None and not _WORKTREE_NAME_RE.match(body.worktree):
        raise HTTPException(status_code=400, detail="Invalid worktree name")

    try:
        entry = process_service.create_business_process(
            name=name, worktree=body.worktree
        )
    except FileExistsError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except (FileNotFoundError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create process: {e}")

    # Push the fresh snapshot to all SSE consumers so the dashboard's
    # sidebar updates without waiting for the workspace watcher tick.
    try:
        await event_broadcaster.broadcast(
            "processes", process_service.get_all_processes()
        )
    except Exception:
        pass

    return entry
