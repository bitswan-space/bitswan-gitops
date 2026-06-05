"""
Shared "deploy a set of automation sources under one task" runner.

Used by the auto-deploy hooks (BP creation, worktree creation, worktree sync)
and the manual `/automations/deploy-changed` endpoint. Lives in its own leaf
module — `routes/automations.py` imports from `routes/agent.py`, so a helper
shared by both (plus `routes/processes.py` and `routes/worktrees.py`) must not
live in either route module. Imports only leaf modules; the AutomationService
singleton is resolved lazily inside `spawn_set_deploy`.

Unlike the strict `/automations/deploy-bp` endpoint (409 if ANY member is
already deploying), `spawn_set_deploy` is best-effort: members that are
already deploying are filtered out and reported as `skipped`, and every
failure mode is returned in the result dict instead of raised — hooks must
never break their parent HTTP operation.
"""

import asyncio
import logging

from app.deploy_manager import DeployStatus, DeployStep, deploy_manager
from app.event_broadcaster import event_broadcaster

logger = logging.getLogger(__name__)

# Strong references to background deploy tasks — prevents GC before completion.
_bg_tasks: set[asyncio.Task] = set()


def _spawn_bg(coro) -> asyncio.Task:
    t = asyncio.create_task(coro)
    _bg_tasks.add(t)
    t.add_done_callback(_bg_tasks.discard)
    return t


async def _run_set_deploy_with_progress(
    task_id: str,
    label: str,
    members: list[dict],
    deployment_ids: list[str],
    service,
    stage: str,
    worktree: str | None,
    commit_subject: str | None,
):
    """Background coroutine driving `deploy_source_set` with progress
    broadcasting. Mirrors `_run_bp_deploy_with_progress`; on terminal status
    `deploy_manager.update_task` releases every member lock.
    """

    async def progress_callback(step: str, message: str, current: int | None = None):
        deploy_step = DeployStep(step)
        if current is not None:
            await deploy_manager.set_current(task_id, current)
        await deploy_manager.update_task(
            task_id,
            status=DeployStatus.IN_PROGRESS,
            step=deploy_step,
            message=message,
        )
        task = deploy_manager.get_task(task_id)
        if task:
            await event_broadcaster.broadcast("deploy_progress", task.to_dict())

    async def _broadcast_task():
        task = deploy_manager.get_task(task_id)
        if task:
            await event_broadcaster.broadcast("deploy_progress", task.to_dict())

    try:
        await deploy_manager.update_task(
            task_id,
            status=DeployStatus.IN_PROGRESS,
            message=f"Deploying {label}...",
        )
        await _broadcast_task()

        await service.deploy_source_set(
            label=label,
            members=members,
            stage=stage,
            worktree=worktree,
            commit_subject=commit_subject,
            progress_callback=progress_callback,
        )

        await deploy_manager.update_task(
            task_id,
            status=DeployStatus.COMPLETED,
            step=DeployStep.DONE,
            message=f"{label} deployed successfully",
        )
        await _broadcast_task()
    except Exception as exc:
        logger.exception("Set deploy failed for %s (task %s)", label, task_id)
        error_detail = str(exc)
        if hasattr(exc, "detail"):
            error_detail = exc.detail
        await deploy_manager.update_task(
            task_id,
            status=DeployStatus.FAILED,
            error=error_detail,
            message=f"{label} deployment failed",
        )
        await _broadcast_task()


async def spawn_set_deploy(
    label: str,
    members: list[dict],
    stage: str,
    worktree: str | None = None,
    commit_subject: str | None = None,
    service=None,
) -> dict:
    """Reserve the deployable members under one task and spawn the background
    set deploy. Never raises.

    Members already being deployed are dropped into `skipped` (logged) rather
    than failing the batch. Returns:
      * `{"deploy": {"task_id", "deployment_ids"}, "skipped": [...]}` on spawn,
      * `{"deploy": None, "reason": "no_members"|"all_in_progress"|"conflict",
         "skipped": [...]}` when there is nothing to do,
      * `{"deploy": None, "error": "..."}` on unexpected failure.
    """
    try:
        if service is None:
            # Lazy import keeps this module a leaf for the route modules.
            from app.dependencies import get_automation_service

            service = get_automation_service()

        if not members:
            return {"deploy": None, "reason": "no_members", "skipped": []}

        pairs = [(m, service.deployment_id_for(m, stage)) for m in members]
        skipped = [did for _, did in pairs if deploy_manager.is_deploying(did)]
        candidates = [(m, did) for m, did in pairs if did not in skipped]
        if skipped:
            logger.info(
                "Set deploy %s: skipping already-deploying members: %s",
                label,
                skipped,
            )
        if not candidates:
            return {"deploy": None, "reason": "all_in_progress", "skipped": skipped}

        members_f = [m for m, _ in candidates]
        ids_f = [did for _, did in candidates]
        task, conflict = await deploy_manager.create_bp_task(label, ids_f)
        if task is None:
            # Race: a member got reserved between the filter and the
            # reservation. Retry once without the conflicting id.
            logger.info(
                "Set deploy %s: member %s reserved concurrently, retrying without it",
                label,
                conflict,
            )
            skipped = skipped + [conflict]
            members_f = [m for m, did in candidates if did != conflict]
            ids_f = [did for _, did in candidates if did != conflict]
            if not ids_f:
                return {
                    "deploy": None,
                    "reason": "all_in_progress",
                    "skipped": skipped,
                }
            task, conflict = await deploy_manager.create_bp_task(label, ids_f)
            if task is None:
                skipped = skipped + [conflict]
                return {"deploy": None, "reason": "conflict", "skipped": skipped}

        _spawn_bg(
            _run_set_deploy_with_progress(
                task.task_id,
                label,
                members_f,
                ids_f,
                service,
                stage,
                worktree,
                commit_subject,
            )
        )
        return {
            "deploy": {"task_id": task.task_id, "deployment_ids": ids_f},
            "skipped": skipped,
        }
    except Exception as exc:
        logger.exception("spawn_set_deploy failed for %s", label)
        return {"deploy": None, "error": str(exc)}
