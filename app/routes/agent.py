import asyncio
import json
import logging
import os
import re
import toml

from fastapi import APIRouter, Depends, HTTPException, Query, Security
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from app.async_docker import get_async_docker_client, DockerError
from app.dependencies import get_automation_service
from app.services.automation_service import AutomationService
from app.utils import (
    call_git_command,
    call_git_command_with_output,
    GitLockContext,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent", tags=["agent"])

security = HTTPBearer()

# Pattern for valid live-dev deployment IDs
LIVE_DEV_PATTERN = re.compile(r"^.+-wt-.+-live-dev$")

# Cached agent secret — resolved lazily from the coding agent container
_cached_agent_secret: str | None = None


def _resolve_agent_secret() -> str:
    """Get the agent secret, discovering it from the running coding agent container if needed."""
    global _cached_agent_secret

    # 1. Already cached
    if _cached_agent_secret:
        return _cached_agent_secret

    # 2. Set in our environment (e.g. by ensure_coding_agent)
    from_env = os.environ.get("BITSWAN_GITOPS_AGENT_SECRET", "")
    if from_env:
        _cached_agent_secret = from_env
        return from_env

    # 3. Discover from the running coding agent container
    workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace")
    agent_container_name = f"{workspace_name}-coding-agent"
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "inspect", "--format",
             '{{range .Config.Env}}{{println .}}{{end}}',
             agent_container_name],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                if line.startswith("BITSWAN_GITOPS_AGENT_SECRET="):
                    secret = line.split("=", 1)[1]
                    if secret:
                        _cached_agent_secret = secret
                        os.environ["BITSWAN_GITOPS_AGENT_SECRET"] = secret
                        logger.info("Discovered agent secret from coding agent container")
                        return secret
    except Exception as e:
        logger.debug("Failed to inspect coding agent container: %s", e)

    return ""


def verify_agent_token(
    credentials: HTTPAuthorizationCredentials = Security(security),
):
    agent_secret = _resolve_agent_secret()
    if not agent_secret or credentials.credentials != agent_secret:
        raise HTTPException(status_code=401, detail="Invalid agent token")


def _validate_deployment_id(deployment_id: str):
    """Validate that deployment_id matches the *-wt-*-live-dev pattern."""
    if not LIVE_DEV_PATTERN.match(deployment_id):
        raise HTTPException(
            status_code=403,
            detail=f"Deployment '{deployment_id}' is not a valid live-dev worktree deployment",
        )


def _get_workspace_dir() -> str:
    """Return the workspace repository directory (the main git repo)."""
    return os.environ.get("BITSWAN_WORKSPACE_REPO_DIR", "/workspace-repo")


def _get_worktrees_base() -> str:
    return os.path.join(_get_workspace_dir(), "worktrees")


# --- Deployment endpoints ---


def _sanitize_name(name: str) -> str:
    """Sanitize an automation source name for use in deployment IDs."""
    return re.sub(r"[^a-z0-9-]", "-", name.lower()).strip("-")


def _scan_worktree_automations(worktree: str) -> list[dict]:
    """Scan the worktree filesystem for automation sources (automation.toml)."""
    worktree_path = os.path.join(_get_workspace_dir(), "worktrees", worktree)
    if not os.path.isdir(worktree_path):
        return []

    skip_dirs = {"templates", "worktrees", ".git"}
    results = []
    seen_ids: set[str] = set()
    for root, dirs, files in os.walk(worktree_path):
        # Prune directories we should never recurse into
        dirs[:] = [d for d in dirs if d not in skip_dirs]
        if "automation.toml" in files:
            rel_path = os.path.relpath(root, worktree_path)
            source_name = os.path.basename(root)
            sanitized = _sanitize_name(source_name)
            deployment_id = f"{sanitized}-wt-{worktree}-live-dev"
            if deployment_id in seen_ids:
                continue
            seen_ids.add(deployment_id)
            results.append({
                "deployment_id": deployment_id,
                "automation_name": source_name,
                "relative_path": f"worktrees/{worktree}/{rel_path}",
                "source_path": root,
            })
    return results


@router.get("/deployments")
async def list_agent_deployments(
    worktree: str = Query(None),
    _token=Depends(verify_agent_token),
):
    """List deployments for a worktree, including those not yet started."""
    if not worktree:
        raise HTTPException(status_code=400, detail="worktree parameter is required")

    # Scan filesystem for all automation sources in this worktree
    sources = _scan_worktree_automations(worktree)

    # Query running containers to get their state
    workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace-local")
    gitops_domain = os.environ.get("BITSWAN_GITOPS_DOMAIN", "")
    docker_client = get_async_docker_client()
    running_states: dict[str, str] = {}

    try:
        containers = await docker_client.list_containers(
            all=True,
            filters={"label": [f"gitops.workspace={workspace_name}"]},
        )
        for container in containers:
            labels = container.get("Labels", {})
            dep_id = labels.get("gitops.deployment_id", "")
            if dep_id.endswith(f"-wt-{worktree}-live-dev"):
                running_states[dep_id] = container.get("State", "unknown")
    except DockerError:
        pass  # If Docker query fails, we still show sources as "not deployed"

    def _make_url(dep_id):
        if gitops_domain:
            return f"https://{workspace_name}-{dep_id}.{gitops_domain}"
        return ""

    # Merge: filesystem sources + running state
    result = []
    for src in sources:
        dep_id = src["deployment_id"]
        state = running_states.pop(dep_id, "not deployed")
        result.append({
            "deployment_id": dep_id,
            "state": state,
            "automation_name": src["automation_name"],
            "url": _make_url(dep_id),
        })

    # Include any running containers not found on filesystem (orphaned)
    for dep_id, state in running_states.items():
        result.append({
            "deployment_id": dep_id,
            "state": state,
            "automation_name": dep_id,
            "url": _make_url(dep_id),
        })

    return result


class StartDeploymentRequest(BaseModel):
    deployment_id: str


@router.post("/deployments/start")
async def start_agent_deployment(
    body: StartDeploymentRequest,
    worktree: str = Query(...),
    automation_service: AutomationService = Depends(get_automation_service),
    _token=Depends(verify_agent_token),
):
    """Start a live-dev deployment for a worktree automation."""
    _validate_deployment_id(body.deployment_id)

    # Find the matching automation source on the filesystem
    sources = _scan_worktree_automations(worktree)
    source = next((s for s in sources if s["deployment_id"] == body.deployment_id), None)
    if not source:
        raise HTTPException(
            status_code=404,
            detail=f"No automation source found for deployment '{body.deployment_id}' in worktree '{worktree}'",
        )

    # Guard: reject if already deploying
    from app.deploy_manager import deploy_manager

    if deploy_manager.is_deploying(body.deployment_id):
        raise HTTPException(
            status_code=409,
            detail=f"Deployment {body.deployment_id} is already in progress",
        )

    task = await deploy_manager.create_task(body.deployment_id)
    if task is None:
        raise HTTPException(
            status_code=409,
            detail=f"Deployment {body.deployment_id} is already in progress",
        )

    # Only send minimal info — the gitops service reads automation.toml
    # directly from the workspace filesystem for live-dev config
    deploy_kwargs = dict(
        deployment_id=body.deployment_id,
        checksum="live-dev",
        stage="live-dev",
        relative_path=source["relative_path"],
        deployed_by="agent@bitswan.local",
    )

    async def _run_deploy():
        try:
            await deploy_manager.update_task(
                task.task_id, message="Starting live-dev deployment..."
            )
            await automation_service.deploy_automation(**deploy_kwargs)
            await deploy_manager.update_task(
                task.task_id, message="Live-dev deployment completed"
            )
        except Exception as exc:
            logger.exception(
                "Live-dev deploy failed for %s (task %s)",
                body.deployment_id,
                task.task_id,
            )
            await deploy_manager.update_task(
                task.task_id, error=str(exc), message="Deployment failed"
            )

    asyncio.create_task(_run_deploy())

    return {
        "task_id": task.task_id,
        "deployment_id": body.deployment_id,
        "status": "pending",
    }


@router.get("/deployments/{deployment_id}/logs")
async def stream_deployment_logs(
    deployment_id: str,
    lines: int = Query(200, ge=1, le=10000),
    since: int = Query(0, ge=0),
    automation_service: AutomationService = Depends(get_automation_service),
    _token=Depends(verify_agent_token),
):
    _validate_deployment_id(deployment_id)
    return StreamingResponse(
        automation_service.stream_automation_logs(
            deployment_id, lines=lines, since=since
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/deployments/{deployment_id}/restart")
async def restart_deployment(
    deployment_id: str,
    automation_service: AutomationService = Depends(get_automation_service),
    _token=Depends(verify_agent_token),
):
    _validate_deployment_id(deployment_id)
    return await automation_service.restart_automation(deployment_id)


@router.post("/deployments/{deployment_id}/build-and-restart")
async def build_and_restart_deployment(
    deployment_id: str,
    automation_service: AutomationService = Depends(get_automation_service),
    _token=Depends(verify_agent_token),
):
    _validate_deployment_id(deployment_id)

    # Trigger deploy which handles image build + restart
    from app.deploy_manager import deploy_manager

    # Guard: reject if already deploying
    if deploy_manager.is_deploying(deployment_id):
        raise HTTPException(
            status_code=409,
            detail=f"Deployment {deployment_id} is already in progress",
        )

    task = await deploy_manager.create_task(deployment_id)
    if task is None:
        raise HTTPException(
            status_code=409,
            detail=f"Deployment {deployment_id} is already in progress",
        )

    # Spawn background deploy task
    async def _run_build_and_restart():
        try:
            await deploy_manager.update_task(
                task.task_id, message="Starting build and restart..."
            )
            await automation_service.deploy_automation(
                deployment_id=deployment_id, stage="live-dev"
            )
            await deploy_manager.update_task(
                task.task_id, message="Build and restart completed"
            )
        except Exception as exc:
            logger.exception(
                "Build and restart failed for %s (task %s)",
                deployment_id,
                task.task_id,
            )
            await deploy_manager.update_task(
                task.task_id, error=str(exc), message="Build and restart failed"
            )

    asyncio.create_task(_run_build_and_restart())

    return {
        "task_id": task.task_id,
        "deployment_id": deployment_id,
        "status": "pending",
    }


# --- Worktree commit endpoint ---


class CommitRequest(BaseModel):
    message: str
    author_email: str = "agent@bitswan.local"


@router.post("/worktrees/{worktree_name}/commit")
async def commit_worktree(
    worktree_name: str,
    body: CommitRequest,
    _token=Depends(verify_agent_token),
):
    worktree_path = os.path.join(_get_worktrees_base(), worktree_name)

    if not os.path.exists(worktree_path):
        raise HTTPException(
            status_code=404, detail=f"Worktree '{worktree_name}' not found"
        )

    async with GitLockContext(timeout=15.0):
        # Stage all changes
        success = await call_git_command("git", "add", "-A", cwd=worktree_path)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to stage changes")

        # Commit with the provided message and author
        author = f"{body.author_email} <{body.author_email}>"
        stdout, stderr, rc = await call_git_command_with_output(
            "git",
            "commit",
            "-m",
            body.message,
            "--author",
            author,
            cwd=worktree_path,
        )
        if rc != 0:
            # Check if it's just "nothing to commit"
            if "nothing to commit" in stdout or "nothing to commit" in stderr:
                raise HTTPException(
                    status_code=400, detail="Nothing to commit"
                )
            raise HTTPException(
                status_code=500,
                detail=f"Failed to commit: {stderr.strip()}",
            )

    # Get the commit hash
    hash_stdout, _, hash_rc = await call_git_command_with_output(
        "git", "rev-parse", "HEAD", cwd=worktree_path
    )
    commit_hash = hash_stdout.strip() if hash_rc == 0 else "unknown"

    return {"status": "success", "commit_hash": commit_hash}


# --- VCS query endpoints ---


@router.get("/worktrees/{worktree_name}/status")
async def worktree_status(
    worktree_name: str,
    _token=Depends(verify_agent_token),
):
    worktree_path = os.path.join(_get_worktrees_base(), worktree_name)
    if not os.path.exists(worktree_path):
        raise HTTPException(status_code=404, detail=f"Worktree '{worktree_name}' not found")

    stdout, stderr, rc = await call_git_command_with_output(
        "git", "status", cwd=worktree_path
    )
    if rc != 0:
        raise HTTPException(status_code=500, detail=f"git status failed: {stderr.strip()}")
    return {"output": stdout}


@router.get("/worktrees/{worktree_name}/log")
async def worktree_log(
    worktree_name: str,
    n: int = Query(20, ge=1, le=200),
    _token=Depends(verify_agent_token),
):
    worktree_path = os.path.join(_get_worktrees_base(), worktree_name)
    if not os.path.exists(worktree_path):
        raise HTTPException(status_code=404, detail=f"Worktree '{worktree_name}' not found")

    stdout, stderr, rc = await call_git_command_with_output(
        "git", "log", f"--oneline", f"-{n}", cwd=worktree_path
    )
    if rc != 0:
        raise HTTPException(status_code=500, detail=f"git log failed: {stderr.strip()}")
    return {"output": stdout}


@router.get("/worktrees/{worktree_name}/diff")
async def worktree_diff(
    worktree_name: str,
    path: str = Query(None),
    _token=Depends(verify_agent_token),
):
    worktree_path = os.path.join(_get_worktrees_base(), worktree_name)
    if not os.path.exists(worktree_path):
        raise HTTPException(status_code=404, detail=f"Worktree '{worktree_name}' not found")

    git_args = ["git", "diff", "HEAD"]
    if path:
        git_args += ["--", path]
    stdout, stderr, rc = await call_git_command_with_output(
        *git_args, cwd=worktree_path
    )
    if rc != 0:
        raise HTTPException(status_code=500, detail=f"git diff failed: {stderr.strip()}")
    return {"output": stdout}


# --- Docker exec endpoint ---


class ExecRequest(BaseModel):
    command: list[str]


@router.post("/deployments/{deployment_id}/exec")
async def exec_in_deployment(
    deployment_id: str,
    body: ExecRequest,
    _token=Depends(verify_agent_token),
):
    _validate_deployment_id(deployment_id)

    docker_client = get_async_docker_client()
    workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace-local")

    # Find the container
    try:
        containers = await docker_client.list_containers(
            all=False,
            filters={
                "label": [
                    f"gitops.deployment_id={deployment_id}",
                    f"gitops.workspace={workspace_name}",
                ]
            },
        )
    except DockerError as e:
        raise HTTPException(status_code=500, detail=f"Docker error: {str(e)}")

    if not containers:
        raise HTTPException(
            status_code=404,
            detail=f"No running container found for deployment '{deployment_id}'",
        )

    container_id = containers[0].get("Id")

    try:
        exec_id = await docker_client.exec_create(container_id, body.command)
        output = await docker_client.exec_start(exec_id)
        exec_info = await docker_client.exec_inspect(exec_id)
        exit_code = exec_info.get("ExitCode", -1)
    except DockerError as e:
        raise HTTPException(status_code=500, detail=f"Docker exec error: {str(e)}")

    return {
        "exit_code": exit_code,
        "output": output,
    }


