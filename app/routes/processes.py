"""
REST surface over the workspace's business processes.

Reads the in-memory cache maintained by `ProcessService`; the cache is kept
fresh by the workspace + worktree file-system watchers in `lifespan.py`.
Same data is broadcast over `/events/stream` as a `processes` event for
push-style consumers (the workspace dashboard).
"""

import logging
import os
import re

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.dependencies import get_automation_service
from app.deploy_runner import spawn_set_deploy
from app.event_broadcaster import event_broadcaster
from app.mqtt_processes import process_service
from app.services import template_service
from app.services.automation_service import AutomationService

logger = logging.getLogger(__name__)

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
async def create_process(
    body: CreateProcessRequest,
    automation_service: AutomationService = Depends(get_automation_service),
) -> dict:
    """Create a new business-process directory with auto-setup.

    Scaffolds `process.toml` + `README.md` under either `<workspace_repo>/<name>/`
    or `<workspace_repo>/worktrees/<worktree>/<name>/`. The new BP surfaces
    over the SSE feed within the same response (this route refreshes the
    cache + broadcasts inline instead of waiting for the filesystem watcher
    to debounce).

    Auto-setup (best-effort): the default template group
    (`BITSWAN_DEFAULT_TEMPLATE_GROUP`, default `BitSwanInternalAppGolang`) is
    scaffolded into the new BP, and its deploy is kicked off in the background
    — `dev` stage for a main BP, `live-dev` for a worktree BP. Failures never
    fail the BP creation; they surface in the `setup_error` response field.
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

    # Auto-setup: scaffold the default template group into the new BP and
    # kick off its deploy in the background. Best-effort — the BP was already
    # created; any failure is logged + reported via `setup_error`.
    automations_created: list[str] = []
    deploy_task_id: str | None = None
    setup_error: str | None = None
    try:
        group_id = os.environ.get(
            "BITSWAN_DEFAULT_TEMPLATE_GROUP", "BitSwanInternalAppGolang"
        )
        workspace_root = os.environ.get("BITSWAN_WORKSPACE_REPO_DIR", "/workspace-repo")
        created = await template_service.create_automation_from_template(
            workspace_root=workspace_root,
            bp=name,
            group_id=group_id,
            worktree=body.worktree,
        )
        automations_created = [c["name"] for c in created.get("created", [])]

        # Inline cache refresh + broadcast (mirrors routes/templates.py) so
        # the new automation cards appear without waiting for the FS watcher.
        try:
            await automation_service.refresh(body.worktree)
            automations = await automation_service.get_automations()
            data = [
                a.model_dump(mode="json") if hasattr(a, "model_dump") else a
                for a in automations
            ]
            await event_broadcaster.broadcast("automations", data)
        except Exception:
            logger.exception("Failed to broadcast automations after BP scaffold")

        stage = "live-dev" if body.worktree else "dev"
        members = automation_service.members_for_bp(
            name, worktree=body.worktree, stage=stage
        )
        res = await spawn_set_deploy(
            label=name,
            members=members,
            stage=stage,
            worktree=body.worktree,
            service=automation_service,
        )
        if res.get("deploy"):
            deploy_task_id = res["deploy"]["task_id"]
        elif res.get("error"):
            setup_error = res["error"]
    except Exception as e:
        logger.exception("BP auto-setup failed for %s", name)
        setup_error = str(e)

    entry["automations_created"] = automations_created
    entry["deploy_task_id"] = deploy_task_id
    if setup_error:
        entry["setup_error"] = setup_error
    return entry
