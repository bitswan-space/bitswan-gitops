import asyncio
import logging
import os
import re

from fastapi import APIRouter, Depends, HTTPException, Query, Security
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from app.async_docker import get_async_docker_client, DockerError
from app.dependencies import get_automation_service
from app.services.automation_service import (
    AutomationService,
    make_hostname_label,
)
from app.utils import (
    call_git_command,
    call_git_command_with_output,
    GitLockContext,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent", tags=["agent"])

security = HTTPBearer()

# Pattern for valid worktree live-dev deployment IDs
# Format: {name}-wt-{worktree}-{bp}-live-dev
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
            [
                "docker",
                "inspect",
                "--format",
                "{{range .Config.Env}}{{println .}}{{end}}",
                agent_container_name,
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                if line.startswith("BITSWAN_GITOPS_AGENT_SECRET="):
                    secret = line.split("=", 1)[1]
                    if secret:
                        _cached_agent_secret = secret
                        os.environ["BITSWAN_GITOPS_AGENT_SECRET"] = secret
                        logger.info(
                            "Discovered agent secret from coding agent container"
                        )
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


def _scan_automations(worktree: str | None = None) -> list[dict]:
    """Scan the filesystem for automation sources (automation.toml).

    If *worktree* is given, scans the worktree directory.
    Otherwise scans the main workspace directory.
    """
    workspace_dir = _get_workspace_dir()
    if worktree:
        scan_root = os.path.join(workspace_dir, "worktrees", worktree)
    else:
        scan_root = workspace_dir
    if not os.path.isdir(scan_root):
        return []

    skip_dirs = {"templates", "worktrees", ".git"}
    results = []
    seen_ids: set[str] = set()
    for root, dirs, files in os.walk(scan_root):
        dirs[:] = [d for d in dirs if d not in skip_dirs]
        if "automation.toml" in files:
            rel_path = os.path.relpath(root, scan_root)
            source_name = os.path.basename(root)
            sanitized = _sanitize_name(source_name)
            rel_parts = rel_path.replace("\\", "/").split("/")
            bp_name = _sanitize_name(rel_parts[0]) if len(rel_parts) >= 2 else ""
            bp_prefix = f"{bp_name}-" if bp_name else ""

            if worktree:
                bp_suffix = f"-{bp_name}" if bp_name else ""
                context = f"wt-{worktree}{bp_suffix}"
                deployment_id = f"{sanitized}-{context}-live-dev"
                relative_path = f"worktrees/{worktree}/{rel_path}"
                deploy_stage = "live-dev"
            else:
                context = bp_name  # just the BP name (or empty)
                deployment_id = f"{sanitized}-{bp_prefix}live-dev"
                relative_path = rel_path
                deploy_stage = "live-dev"

            if deployment_id in seen_ids:
                continue
            seen_ids.add(deployment_id)
            results.append(
                {
                    "deployment_id": deployment_id,
                    "automation_name": sanitized,
                    "display_name": source_name,
                    "context": context,
                    "stage": deploy_stage,
                    "relative_path": relative_path,
                    "source_path": root,
                    "worktree": worktree,
                }
            )
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
    sources = _scan_automations(worktree)

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
            if f"-wt-{worktree}" in dep_id and dep_id.endswith("-live-dev"):
                running_states[dep_id] = container.get("State", "unknown")
    except DockerError:
        pass  # If Docker query fails, we still show sources as "not deployed"

    def _make_url(src):
        if gitops_domain:
            label = make_hostname_label(
                workspace_name, src["automation_name"], src["context"], src["stage"]
            )
            return f"https://{label}.{gitops_domain}"
        return ""

    # Merge: filesystem sources + running state
    result = []
    for src in sources:
        dep_id = src["deployment_id"]
        state = running_states.pop(dep_id, "not deployed")
        result.append(
            {
                "deployment_id": dep_id,
                "state": state,
                "automation_name": src["automation_name"],
                "context": src["context"],
                "stage": src["stage"],
                "relative_path": src["relative_path"],
                "worktree": src["worktree"],
                "url": _make_url(src),
            }
        )

    # Include any running containers not found on filesystem (orphaned)
    for dep_id, state in running_states.items():
        orphan = {"automation_name": dep_id, "context": "", "stage": "live-dev"}
        result.append(
            {
                "deployment_id": dep_id,
                "state": state,
                "automation_name": dep_id,
                "context": "",
                "stage": "live-dev",
                "relative_path": None,
                "worktree": worktree,
                "url": _make_url(orphan),
            }
        )

    return result


class StartDeploymentRequest(BaseModel):
    relative_path: str
    worktree: str | None = None


@router.post("/deployments/start")
async def start_agent_deployment(
    body: StartDeploymentRequest,
    automation_service: AutomationService = Depends(get_automation_service),
    _token=Depends(verify_agent_token),
):
    """Start a live-dev deployment for an automation."""
    sources = _scan_automations(body.worktree)
    source = next(
        (s for s in sources if s["relative_path"] == body.relative_path), None
    )
    if not source:
        ctx = f" in worktree '{body.worktree}'" if body.worktree else ""
        raise HTTPException(
            status_code=404,
            detail=f"No automation source at '{body.relative_path}'{ctx}",
        )

    deployment_id = source["deployment_id"]

    # Guard: reject if already deploying
    from app.deploy_manager import deploy_manager

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

    # Only send minimal info — the gitops service reads automation.toml
    # directly from the workspace filesystem for live-dev config
    deploy_kwargs = dict(
        deployment_id=deployment_id,
        checksum="live-dev",
        stage="live-dev",
        relative_path=source["relative_path"],
        automation_name=source["automation_name"],
        context=source["context"],
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
                deployment_id,
                task.task_id,
            )
            await deploy_manager.update_task(
                task.task_id, error=str(exc), message="Deployment failed"
            )

    asyncio.create_task(_run_deploy())

    workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace-local")
    gitops_domain = os.environ.get("BITSWAN_GITOPS_DOMAIN", "")
    url = ""
    if gitops_domain:
        label = make_hostname_label(
            workspace_name,
            source["automation_name"],
            source["context"],
            source["stage"],
        )
        url = f"https://{label}.{gitops_domain}"

    return {
        "task_id": task.task_id,
        "deployment_id": deployment_id,
        "url": url,
        "status": "pending",
    }


@router.get("/deployments/{deployment_id}/inspect")
async def inspect_deployment(
    deployment_id: str,
    _token=Depends(verify_agent_token),
):
    """Full inspect of a deployment container."""
    _validate_deployment_id(deployment_id)

    docker_client = get_async_docker_client()
    workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace-local")

    try:
        containers = await docker_client.list_containers(
            all=True,
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
            status_code=404, detail=f"No container found for '{deployment_id}'"
        )

    container_id = containers[0].get("Id")
    try:
        info = await docker_client.get_container(container_id)
    except DockerError as e:
        raise HTTPException(status_code=500, detail=f"Docker inspect error: {str(e)}")

    state = info.get("State", {})
    config = info.get("Config", {})
    host_config = info.get("HostConfig", {})
    network_settings = info.get("NetworkSettings", {})

    # Extract useful fields
    networks = {}
    for net_name, net_info in network_settings.get("Networks", {}).items():
        networks[net_name] = {
            "ip": net_info.get("IPAddress", ""),
            "aliases": net_info.get("Aliases", []),
        }

    mounts = []
    for m in info.get("Mounts", []):
        mounts.append(
            {
                "source": m.get("Source", ""),
                "destination": m.get("Destination", ""),
                "mode": m.get("Mode", ""),
                "rw": m.get("RW", True),
            }
        )

    ports = {}
    for port, bindings in (network_settings.get("Ports") or {}).items():
        if bindings:
            ports[port] = [
                {"host_ip": b.get("HostIp", ""), "host_port": b.get("HostPort", "")}
                for b in bindings
            ]
        else:
            ports[port] = None

    return {
        "deployment_id": deployment_id,
        "container_id": container_id[:12],
        "container_name": info.get("Name", "").lstrip("/"),
        "image": config.get("Image", ""),
        "state": {
            "status": state.get("Status", ""),
            "running": state.get("Running", False),
            "started_at": state.get("StartedAt", ""),
            "finished_at": state.get("FinishedAt", ""),
            "exit_code": state.get("ExitCode", 0),
            "restarting": state.get("Restarting", False),
        },
        "networks": networks,
        "ports": ports,
        "mounts": mounts,
        "labels": config.get("Labels", {}),
        "restart_policy": host_config.get("RestartPolicy", {}).get("Name", ""),
    }


@router.get("/deployments/{deployment_id}/env")
async def get_deployment_env(
    deployment_id: str,
    _token=Depends(verify_agent_token),
):
    """Get environment variables for a deployment container (from docker inspect)."""
    _validate_deployment_id(deployment_id)

    docker_client = get_async_docker_client()
    workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace-local")

    try:
        containers = await docker_client.list_containers(
            all=True,
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
            status_code=404, detail=f"No container found for '{deployment_id}'"
        )

    container_id = containers[0].get("Id")
    try:
        info = await docker_client.get_container(container_id)
        env_list = info.get("Config", {}).get("Env", [])
    except DockerError as e:
        raise HTTPException(status_code=500, detail=f"Docker inspect error: {str(e)}")

    # Parse "KEY=VALUE" into dict
    env_vars = {}
    for entry in env_list:
        key, _, value = entry.partition("=")
        env_vars[key] = value

    return {"deployment_id": deployment_id, "env": env_vars}


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
    """Build image and restart deployment. Streams progress as text/plain."""
    _validate_deployment_id(deployment_id)

    from app.deploy_manager import deploy_manager

    if deploy_manager.is_deploying(deployment_id):
        raise HTTPException(
            status_code=409,
            detail=f"Deployment {deployment_id} is already in progress",
        )

    async def _stream():
        from app.services.automation_service import read_bitswan_yaml

        yield "Looking up deployment...\n"
        bs_yaml = read_bitswan_yaml(automation_service.gitops_dir)
        dep_conf = (bs_yaml or {}).get("deployments", {}).get(deployment_id, {})
        relative_path = dep_conf.get("relative_path", "")

        # Build image if image/ directory exists
        if relative_path:
            source_dir = os.path.join(
                automation_service.workspace_repo_dir, relative_path
            )
            image_dir = os.path.join(source_dir, "image")
            if os.path.isdir(image_dir) and os.path.isfile(
                os.path.join(image_dir, "Dockerfile")
            ):
                yield "Building image...\n"
                from app.services.image_service import ImageService

                image_service = ImageService()
                auto_name = os.path.basename(source_dir)
                result = await image_service.create_image(
                    image_tag=auto_name,
                    build_context_path=image_dir,
                )
                tag = result.get("tag", "")
                checksum = tag.split(":sha", 1)[1] if ":sha" in tag else None

                if checksum:
                    async for chunk in image_service.stream_build_logs(checksum):
                        yield chunk

                    build_status = image_service._get_build_status(checksum)
                    if build_status == "failed":
                        yield "ERROR: Image build failed\n"
                        return

                yield f"Image built: {tag}\n"

        yield "Deploying...\n"
        try:
            await automation_service.deploy_automation(
                deployment_id=deployment_id, stage="live-dev"
            )
            yield "Deploy completed successfully\n"
        except Exception as exc:
            yield f"ERROR: {exc}\n"

    return StreamingResponse(_stream(), media_type="text/plain")


@router.get("/images/builds/{checksum}/stream")
async def stream_agent_build_logs(
    checksum: str,
    _token=Depends(verify_agent_token),
):
    """Stream image build logs (proxied from image service)."""
    from app.services.image_service import ImageService

    image_service = ImageService()
    return StreamingResponse(
        image_service.stream_build_logs(checksum), media_type="text/plain"
    )


@router.get("/deployments/{deployment_id}/deploy-status")
async def get_deployment_status(
    deployment_id: str,
    _token=Depends(verify_agent_token),
):
    """Get the active deploy task for a deployment, if any."""
    from app.deploy_manager import deploy_manager

    task_id = deploy_manager._active_deploys.get(deployment_id)
    if not task_id:
        return {"deploying": False}
    task = deploy_manager.get_task(task_id)
    if not task:
        return {"deploying": False}
    return {"deploying": True, **task.to_dict()}


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
                raise HTTPException(status_code=400, detail="Nothing to commit")
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
        raise HTTPException(
            status_code=404, detail=f"Worktree '{worktree_name}' not found"
        )

    stdout, stderr, rc = await call_git_command_with_output(
        "git", "status", cwd=worktree_path
    )
    if rc != 0:
        raise HTTPException(
            status_code=500, detail=f"git status failed: {stderr.strip()}"
        )
    return {"output": stdout}


@router.get("/worktrees/{worktree_name}/log")
async def worktree_log(
    worktree_name: str,
    n: int = Query(20, ge=1, le=200),
    _token=Depends(verify_agent_token),
):
    worktree_path = os.path.join(_get_worktrees_base(), worktree_name)
    if not os.path.exists(worktree_path):
        raise HTTPException(
            status_code=404, detail=f"Worktree '{worktree_name}' not found"
        )

    stdout, stderr, rc = await call_git_command_with_output(
        "git", "log", "--oneline", f"-{n}", cwd=worktree_path
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
        raise HTTPException(
            status_code=404, detail=f"Worktree '{worktree_name}' not found"
        )

    git_args = ["git", "diff", "HEAD"]
    if path:
        git_args += ["--", path]
    stdout, stderr, rc = await call_git_command_with_output(
        *git_args, cwd=worktree_path
    )
    if rc != 0:
        raise HTTPException(
            status_code=500, detail=f"git diff failed: {stderr.strip()}"
        )
    return {"output": stdout}


# --- Rebase and merge endpoint ---


async def _stash_workspace(workspace_dir: str) -> bool:
    """Stash all changes including untracked files. Returns True if a stash was created."""
    before, _, _ = await call_git_command_with_output(
        "git", "stash", "list", cwd=workspace_dir
    )
    count_before = len(before.strip().splitlines()) if before.strip() else 0
    await call_git_command_with_output(
        "git",
        "stash",
        "push",
        "--include-untracked",
        "-m",
        "rebase-merge-stash",
        cwd=workspace_dir,
    )
    after, _, _ = await call_git_command_with_output(
        "git", "stash", "list", cwd=workspace_dir
    )
    count_after = len(after.strip().splitlines()) if after.strip() else 0
    return count_after > count_before


async def _complete_merge(
    workspace_dir: str, worktree_path: str, stash_created: bool
) -> dict:
    """After a successful rebase, fast-forward the default branch and pop stash."""
    # Get default branch
    stdout, stderr, rc = await call_git_command_with_output(
        "git", "rev-parse", "--abbrev-ref", "HEAD", cwd=workspace_dir
    )
    if rc != 0:
        return {
            "status": "error",
            "detail": f"Failed to detect default branch: {stderr.strip()}",
        }
    default_branch = stdout.strip()

    # Get worktree tip
    tip_stdout, _, rc = await call_git_command_with_output(
        "git", "rev-parse", "HEAD", cwd=worktree_path
    )
    if rc != 0:
        return {"status": "error", "detail": "Failed to get worktree HEAD after rebase"}
    tip_sha = tip_stdout.strip()

    # Fast-forward
    stdout, stderr, rc = await call_git_command_with_output(
        "git", "merge", "--ff-only", tip_sha, cwd=workspace_dir
    )
    if rc != 0:
        if stash_created:
            await call_git_command_with_output("git", "stash", "pop", cwd=workspace_dir)
        return {"status": "error", "detail": f"Fast-forward failed: {stderr.strip()}"}

    # Pop stash
    stash_conflict = False
    stash_message = ""
    if stash_created:
        _, pop_stderr, pop_rc = await call_git_command_with_output(
            "git", "stash", "pop", cwd=workspace_dir
        )
        if pop_rc != 0:
            stash_conflict = True
            stash_message = pop_stderr.strip()

    return {
        "status": "success",
        "merged_into": default_branch,
        "tip": tip_sha[:12],
        "stash_conflict": stash_conflict,
        "stash_message": stash_message,
    }


@router.post("/worktrees/{worktree_name}/rebase-and-merge")
async def rebase_and_merge(
    worktree_name: str,
    _token=Depends(verify_agent_token),
):
    """Start rebase of worktree onto default branch. If no conflicts, completes the merge.
    If conflicts occur, returns status='conflicts' with conflict details — resolve the
    files and call rebase-continue."""
    workspace_dir = _get_workspace_dir()
    worktree_path = os.path.join(_get_worktrees_base(), worktree_name)

    if not os.path.exists(worktree_path):
        raise HTTPException(
            status_code=404, detail=f"Worktree '{worktree_name}' not found"
        )

    async with GitLockContext(timeout=30.0):
        # Detect default branch
        stdout, stderr, rc = await call_git_command_with_output(
            "git", "rev-parse", "--abbrev-ref", "HEAD", cwd=workspace_dir
        )
        if rc != 0:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to detect default branch: {stderr.strip()}",
            )
        default_branch = stdout.strip()

        # Stash workspace changes
        stash_created = await _stash_workspace(workspace_dir)

        # Start rebase
        stdout, stderr, rc = await call_git_command_with_output(
            "git", "rebase", default_branch, cwd=worktree_path
        )

        if rc != 0:
            # Check whether this is actually a conflict or some other failure
            conflict_stdout, _, _ = await call_git_command_with_output(
                "git", "diff", "--name-only", "--diff-filter=U", cwd=worktree_path
            )
            conflicted_files = [f for f in conflict_stdout.strip().splitlines() if f]

            if not conflicted_files:
                # Not a conflict — abort the rebase and restore stash
                await call_git_command_with_output(
                    "git", "rebase", "--abort", cwd=worktree_path
                )
                if stash_created:
                    await call_git_command_with_output(
                        "git", "stash", "pop", cwd=workspace_dir
                    )
                rebase_output = f"{stdout.strip()}\n{stderr.strip()}".strip()
                raise HTTPException(
                    status_code=500,
                    detail=f"Rebase failed: {rebase_output}",
                )

            return {
                "status": "conflicts",
                "message": "Rebase paused due to conflicts. Resolve the files and run rebase-continue.",
                "conflicted_files": conflicted_files,
                "rebase_output": f"{stdout.strip()}\n{stderr.strip()}".strip(),
                "default_branch": default_branch,
                "stash_created": stash_created,
            }

        # No conflicts — complete the merge
        result = await _complete_merge(workspace_dir, worktree_path, stash_created)
        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result["detail"])
        return result


@router.post("/worktrees/{worktree_name}/rebase-continue")
async def rebase_continue(
    worktree_name: str,
    _token=Depends(verify_agent_token),
):
    """After resolving conflicts, stage resolved files and continue the rebase.
    If more conflicts arise, returns status='conflicts' again.
    When rebase completes, fast-forwards the default branch."""
    workspace_dir = _get_workspace_dir()
    worktree_path = os.path.join(_get_worktrees_base(), worktree_name)

    if not os.path.exists(worktree_path):
        raise HTTPException(
            status_code=404, detail=f"Worktree '{worktree_name}' not found"
        )

    async with GitLockContext(timeout=30.0):
        # Stage all resolved files
        _, add_stderr, add_rc = await call_git_command_with_output(
            "git", "add", "-A", cwd=worktree_path
        )
        if add_rc != 0:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to stage resolved files: {add_stderr.strip()}",
            )

        # Continue rebase
        stdout, stderr, rc = await call_git_command_with_output(
            "git", "-c", "core.editor=true", "rebase", "--continue", cwd=worktree_path
        )

        if rc != 0:
            # More conflicts
            conflict_stdout, _, _ = await call_git_command_with_output(
                "git", "diff", "--name-only", "--diff-filter=U", cwd=worktree_path
            )
            conflicted_files = [f for f in conflict_stdout.strip().splitlines() if f]

            if conflicted_files:
                return {
                    "status": "conflicts",
                    "message": "More conflicts encountered. Resolve and run rebase-continue again.",
                    "conflicted_files": conflicted_files,
                    "rebase_output": f"{stdout.strip()}\n{stderr.strip()}".strip(),
                }

            # rebase --continue failed but no conflict markers — something else went wrong
            raise HTTPException(
                status_code=500,
                detail=f"Rebase continue failed: {stderr.strip()}\n{stdout.strip()}",
            )

        # Rebase complete — check if there was a stash
        stash_list, _, _ = await call_git_command_with_output(
            "git", "stash", "list", cwd=workspace_dir
        )
        stash_created = "rebase-merge-stash" in stash_list

        result = await _complete_merge(workspace_dir, worktree_path, stash_created)
        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result["detail"])
        return result


@router.post("/worktrees/{worktree_name}/sync")
async def sync_worktree(
    worktree_name: str,
    _token=Depends(verify_agent_token),
):
    """Rebase the worktree branch onto the default branch without modifying the
    default branch.  This brings the worktree up-to-date with main/master.

    If conflicts occur, returns status='conflicts' with the list of files.
    Resolve them and call sync-continue."""
    workspace_dir = _get_workspace_dir()
    worktree_path = os.path.join(_get_worktrees_base(), worktree_name)

    if not os.path.exists(worktree_path):
        raise HTTPException(
            status_code=404, detail=f"Worktree '{worktree_name}' not found"
        )

    async with GitLockContext(timeout=30.0):
        # Detect default branch
        stdout, stderr, rc = await call_git_command_with_output(
            "git", "rev-parse", "--abbrev-ref", "HEAD", cwd=workspace_dir
        )
        if rc != 0:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to detect default branch: {stderr.strip()}",
            )
        default_branch = stdout.strip()

        # Start rebase
        stdout, stderr, rc = await call_git_command_with_output(
            "git", "rebase", default_branch, cwd=worktree_path
        )

        if rc != 0:
            conflict_stdout, _, _ = await call_git_command_with_output(
                "git", "diff", "--name-only", "--diff-filter=U", cwd=worktree_path
            )
            conflicted_files = [f for f in conflict_stdout.strip().splitlines() if f]

            if not conflicted_files:
                # Not a conflict — abort the rebase
                await call_git_command_with_output(
                    "git", "rebase", "--abort", cwd=worktree_path
                )
                rebase_output = f"{stdout.strip()}\n{stderr.strip()}".strip()
                raise HTTPException(
                    status_code=500,
                    detail=f"Rebase failed: {rebase_output}",
                )

            return {
                "status": "conflicts",
                "message": "Rebase paused due to conflicts. Resolve the files and run sync-continue.",
                "conflicted_files": conflicted_files,
                "rebase_output": f"{stdout.strip()}\n{stderr.strip()}".strip(),
            }

        # Rebase succeeded — worktree is now up-to-date
        tip_stdout, tip_stderr, tip_rc = await call_git_command_with_output(
            "git", "rev-parse", "HEAD", cwd=worktree_path
        )
        if tip_rc != 0:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get worktree HEAD after rebase: {tip_stderr.strip()}",
            )
        return {
            "status": "success",
            "tip": tip_stdout.strip()[:12],
        }


@router.post("/worktrees/{worktree_name}/sync-continue")
async def sync_continue(
    worktree_name: str,
    _token=Depends(verify_agent_token),
):
    """Continue a sync rebase after conflicts have been resolved.
    Unlike rebase-continue, this does NOT fast-forward the default branch."""
    worktree_path = os.path.join(_get_worktrees_base(), worktree_name)

    if not os.path.exists(worktree_path):
        raise HTTPException(
            status_code=404, detail=f"Worktree '{worktree_name}' not found"
        )

    async with GitLockContext(timeout=30.0):
        # Stage all resolved files
        _, add_stderr, add_rc = await call_git_command_with_output(
            "git", "add", "-A", cwd=worktree_path
        )
        if add_rc != 0:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to stage resolved files: {add_stderr.strip()}",
            )

        # Continue rebase
        stdout, stderr, rc = await call_git_command_with_output(
            "git", "-c", "core.editor=true", "rebase", "--continue", cwd=worktree_path
        )

        if rc != 0:
            conflict_stdout, _, _ = await call_git_command_with_output(
                "git", "diff", "--name-only", "--diff-filter=U", cwd=worktree_path
            )
            conflicted_files = [f for f in conflict_stdout.strip().splitlines() if f]

            if conflicted_files:
                return {
                    "status": "conflicts",
                    "message": "More conflicts encountered. Resolve and run sync-continue again.",
                    "conflicted_files": conflicted_files,
                    "rebase_output": f"{stdout.strip()}\n{stderr.strip()}".strip(),
                }

            raise HTTPException(
                status_code=500,
                detail=f"Rebase continue failed: {stderr.strip()}\n{stdout.strip()}",
            )

        tip_stdout, tip_stderr, tip_rc = await call_git_command_with_output(
            "git", "rev-parse", "HEAD", cwd=worktree_path
        )
        if tip_rc != 0:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get worktree HEAD after rebase: {tip_stderr.strip()}",
            )
        return {
            "status": "success",
            "tip": tip_stdout.strip()[:12],
        }


@router.post("/worktrees/{worktree_name}/rebase-abort")
async def rebase_abort(
    worktree_name: str,
    _token=Depends(verify_agent_token),
):
    """Abort an in-progress rebase and pop the stash if one was created."""
    workspace_dir = _get_workspace_dir()
    worktree_path = os.path.join(_get_worktrees_base(), worktree_name)

    if not os.path.exists(worktree_path):
        raise HTTPException(
            status_code=404, detail=f"Worktree '{worktree_name}' not found"
        )

    async with GitLockContext(timeout=15.0):
        await call_git_command_with_output(
            "git", "rebase", "--abort", cwd=worktree_path
        )

        # Pop stash if we created one
        stash_list, _, _ = await call_git_command_with_output(
            "git", "stash", "list", cwd=workspace_dir
        )
        if "rebase-merge-stash" in stash_list:
            await call_git_command_with_output("git", "stash", "pop", cwd=workspace_dir)

    return {"status": "aborted", "message": "Rebase aborted and stash restored."}


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
