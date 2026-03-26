import logging
import os
import re
import uuid

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.async_docker import get_async_docker_client, DockerError
from app.utils import (
    call_git_command,
    call_git_command_with_output,
    GitLockContext,
)


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/worktrees", tags=["worktrees"])


def _get_postgres_secrets(stage: str = "dev") -> dict | None:
    """Read Postgres connection info from the secrets file for the given stage."""
    bs_home = os.environ.get("BITSWAN_GITOPS_DIR", "/mnt/repo/pipeline")
    suffix = f"-{stage}" if stage != "production" else ""
    secrets_path = os.path.join(bs_home, "secrets", f"postgres{suffix}")
    if not os.path.exists(secrets_path):
        return None
    info = {}
    with open(secrets_path) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                key, _, value = line.partition("=")
                info[key] = value
    if (
        info.get("POSTGRES_USER")
        and info.get("POSTGRES_PASSWORD")
        and info.get("POSTGRES_HOST")
    ):
        return info
    return None


def _worktree_db_name(worktree_name: str) -> str:
    """Generate a Postgres database name for a worktree."""
    safe = re.sub(r"[^a-z0-9_]", "_", worktree_name.lower())
    return f"postgres_wt_{safe}"


async def _clone_postgres_db(worktree_name: str) -> str | None:
    """Clone the dev Postgres database for a worktree. Returns the new DB name or None."""
    secrets = _get_postgres_secrets("dev")
    if not secrets:
        return None

    user = secrets["POSTGRES_USER"]
    password = secrets["POSTGRES_PASSWORD"]
    source_db = secrets.get("POSTGRES_DB", "postgres")
    new_db = _worktree_db_name(worktree_name)

    docker_client = get_async_docker_client()
    workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace-local")
    container_name = f"{workspace_name}__postgres-dev"

    try:
        # Terminate connections to the source DB so TEMPLATE works
        terminate_sql = (
            f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            f"WHERE datname = '{source_db}' AND pid <> pg_backend_pid();"
        )
        # Clone the database using CREATE DATABASE ... WITH TEMPLATE
        clone_sql = f'CREATE DATABASE "{new_db}" WITH TEMPLATE "{source_db}";'

        containers = await docker_client.list_containers(
            all=False,
            filters={"name": [f"^/{container_name}$"]},
        )
        if not containers:
            logger.warning(
                f"Postgres dev container '{container_name}' not found, skipping DB clone"
            )
            return None

        cid = containers[0]["Id"]

        for sql in [terminate_sql, clone_sql]:
            # exec_create doesn't support environment, so pass PGPASSWORD via sh -c
            exec_id = await docker_client.exec_create(
                cid,
                [
                    "sh",
                    "-c",
                    f"PGPASSWORD='{password}' psql -U {user} -d postgres -c \"{sql}\"",
                ],
            )
            output = await docker_client.exec_start(exec_id)
            info = await docker_client.exec_inspect(exec_id)
            if info.get("ExitCode", 1) != 0 and "already exists" not in (output or ""):
                logger.warning(
                    f"Postgres command failed (exit {info.get('ExitCode')}): {output}"
                )
                if "CREATE DATABASE" in sql:
                    return None

        logger.info(
            f"Cloned Postgres DB '{source_db}' -> '{new_db}' for worktree '{worktree_name}'"
        )
        return new_db
    except Exception as e:
        logger.warning(
            f"Failed to clone Postgres DB for worktree '{worktree_name}': {e}"
        )
        return None


async def _drop_postgres_db(worktree_name: str) -> None:
    """Drop the worktree's Postgres database."""
    secrets = _get_postgres_secrets("dev")
    if not secrets:
        return

    user = secrets["POSTGRES_USER"]
    password = secrets["POSTGRES_PASSWORD"]
    new_db = _worktree_db_name(worktree_name)

    docker_client = get_async_docker_client()
    workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace-local")
    container_name = f"{workspace_name}__postgres-dev"

    try:
        containers = await docker_client.list_containers(
            all=False,
            filters={"name": [f"^/{container_name}$"]},
        )
        if not containers:
            return

        cid = containers[0]["Id"]

        # Terminate connections first
        terminate_sql = (
            f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            f"WHERE datname = '{new_db}' AND pid <> pg_backend_pid();"
        )
        for sql in [terminate_sql, f'DROP DATABASE IF EXISTS "{new_db}";']:
            exec_id = await docker_client.exec_create(
                cid,
                [
                    "sh",
                    "-c",
                    f"PGPASSWORD='{password}' psql -U {user} -d postgres -c \"{sql}\"",
                ],
            )
            await docker_client.exec_start(exec_id)

        logger.info(f"Dropped Postgres DB '{new_db}' for worktree '{worktree_name}'")
    except Exception as e:
        logger.warning(
            f"Failed to drop Postgres DB for worktree '{worktree_name}': {e}"
        )


def _get_workspace_dir() -> str:
    """Return the workspace repository directory (the main git repo)."""
    return os.environ.get("BITSWAN_WORKSPACE_REPO_DIR", "/workspace-repo")


def _get_worktrees_base() -> str:
    return os.path.join(_get_workspace_dir(), "worktrees")


class CreateWorktreeRequest(BaseModel):
    branch_name: str
    base_branch: str = None  # auto-detect from current HEAD if not provided


@router.post("/create")
async def create_worktree(body: CreateWorktreeRequest):
    # Validate branch_name: alphanumeric + hyphens only
    if not re.match(r"^[a-zA-Z0-9][a-zA-Z0-9\-]*$", body.branch_name):
        raise HTTPException(
            status_code=400,
            detail="branch_name must be alphanumeric with hyphens only",
        )

    workspace_dir = _get_workspace_dir()
    worktrees_base = _get_worktrees_base()
    worktree_path = os.path.join(worktrees_base, body.branch_name)

    if os.path.exists(worktree_path):
        raise HTTPException(
            status_code=409,
            detail=f"Worktree '{body.branch_name}' already exists",
        )

    # Auto-detect base branch from current HEAD if not provided
    base_branch = body.base_branch
    if not base_branch:
        stdout, _, rc = await call_git_command_with_output(
            "git", "rev-parse", "--abbrev-ref", "HEAD", cwd=workspace_dir
        )
        base_branch = stdout.strip() if rc == 0 and stdout.strip() else "main"

    # Ensure worktrees directory exists
    os.makedirs(worktrees_base, exist_ok=True)

    # Ensure worktrees/ is in .gitignore so worktree contents aren't tracked
    gitignore_path = os.path.join(workspace_dir, ".gitignore")
    try:
        existing = ""
        if os.path.exists(gitignore_path):
            with open(gitignore_path) as f:
                existing = f.read()
        if "worktrees/" not in existing:
            with open(gitignore_path, "a") as f:
                if existing and not existing.endswith("\n"):
                    f.write("\n")
                f.write("worktrees/\n")
    except OSError:
        pass  # best-effort

    async with GitLockContext(timeout=15.0):
        # Create the branch from base
        success = await call_git_command(
            "git", "branch", body.branch_name, base_branch, cwd=workspace_dir
        )
        if not success:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create branch '{body.branch_name}' from '{base_branch}'",
            )

        # Create the worktree
        success = await call_git_command(
            "git",
            "worktree",
            "add",
            worktree_path,
            body.branch_name,
            cwd=workspace_dir,
        )
        if not success:
            # Clean up the branch if worktree creation failed
            await call_git_command(
                "git", "branch", "-D", body.branch_name, cwd=workspace_dir
            )
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create worktree for branch '{body.branch_name}'",
            )

    # Clone Postgres dev database for this worktree (best-effort)
    cloned_db = await _clone_postgres_db(body.branch_name)

    result = {"name": body.branch_name, "path": worktree_path}
    if cloned_db:
        result["postgres_db"] = cloned_db
    return result


@router.get("/")
async def list_worktrees():
    workspace_dir = _get_workspace_dir()
    worktrees_base = _get_worktrees_base()

    # Get worktree list from git
    stdout, stderr, rc = await call_git_command_with_output(
        "git", "worktree", "list", "--porcelain", cwd=workspace_dir
    )

    if rc != 0:
        raise HTTPException(
            status_code=500, detail=f"Failed to list worktrees: {stderr}"
        )

    # Parse porcelain output
    worktrees = []
    current_wt = {}
    for line in stdout.split("\n"):
        line = line.strip()
        if not line:
            if current_wt and current_wt.get("path"):
                worktrees.append(current_wt)
            current_wt = {}
            continue
        if line.startswith("worktree "):
            current_wt["path"] = line[len("worktree ") :]
        elif line.startswith("branch "):
            current_wt["branch"] = line[len("branch ") :]
        elif line.startswith("HEAD "):
            current_wt["head"] = line[len("HEAD ") :]
    # Capture last entry
    if current_wt and current_wt.get("path"):
        worktrees.append(current_wt)

    # Filter to only worktrees in the worktrees/ subdirectory
    result = []
    for wt in worktrees:
        wt_path = wt.get("path", "")
        if not wt_path.startswith(worktrees_base):
            continue

        name = os.path.basename(wt_path)
        branch = wt.get("branch", "").replace("refs/heads/", "")

        # Get last commit info
        commit_hash = ""
        commit_message = ""
        if branch:
            log_stdout, _, log_rc = await call_git_command_with_output(
                "git", "log", "-1", "--format=%H %s", branch, cwd=workspace_dir
            )
            if log_rc == 0 and log_stdout.strip():
                parts = log_stdout.strip().split(" ", 1)
                commit_hash = parts[0] if len(parts) > 0 else ""
                commit_message = parts[1] if len(parts) > 1 else ""

        # Check if .requirements.json exists
        has_requirements = os.path.exists(os.path.join(wt_path, ".requirements.json"))

        result.append(
            {
                "name": name,
                "branch": branch,
                "commit_hash": commit_hash,
                "commit_message": commit_message,
                "has_requirements": has_requirements,
            }
        )

    return result


class MergeWorktreeResponse(BaseModel):
    status: str
    message: str


@router.post("/{name}/merge")
async def merge_worktree(name: str):
    workspace_dir = _get_workspace_dir()
    worktree_path = os.path.join(_get_worktrees_base(), name)

    if not os.path.exists(worktree_path):
        raise HTTPException(status_code=404, detail=f"Worktree '{name}' not found")

    async with GitLockContext(timeout=15.0):
        # Detect the main branch
        stdout, _, rc = await call_git_command_with_output(
            "git", "symbolic-ref", "refs/remotes/origin/HEAD", cwd=workspace_dir
        )
        if rc == 0 and stdout.strip():
            main_branch = stdout.strip().replace("refs/remotes/origin/", "")
        else:
            main_branch = "main"

        # Checkout main branch
        success = await call_git_command(
            "git", "checkout", main_branch, cwd=workspace_dir
        )
        if not success:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to checkout '{main_branch}'",
            )

        # Merge the worktree branch
        stdout, stderr, rc = await call_git_command_with_output(
            "git", "merge", name, cwd=workspace_dir
        )
        if rc != 0:
            # Abort the merge on failure
            await call_git_command("git", "merge", "--abort", cwd=workspace_dir)
            raise HTTPException(
                status_code=409,
                detail=f"Merge failed: {stderr.strip()}",
            )

        # Remove the worktree
        await call_git_command(
            "git", "worktree", "remove", worktree_path, cwd=workspace_dir
        )

        # Delete the branch
        await call_git_command("git", "branch", "-d", name, cwd=workspace_dir)

    return {
        "status": "success",
        "message": f"Branch '{name}' merged into '{main_branch}' and worktree removed",
    }


@router.delete("/{name}")
async def delete_worktree(name: str):
    workspace_dir = _get_workspace_dir()
    worktree_path = os.path.join(_get_worktrees_base(), name)

    if not os.path.exists(worktree_path):
        raise HTTPException(status_code=404, detail=f"Worktree '{name}' not found")

    # Drop the worktree's Postgres database (best-effort)
    await _drop_postgres_db(name)

    async with GitLockContext(timeout=15.0):
        # Force-remove the worktree
        success = await call_git_command(
            "git", "worktree", "remove", worktree_path, "--force", cwd=workspace_dir
        )
        if not success:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to remove worktree '{name}'",
            )

        # Delete the branch
        await call_git_command("git", "branch", "-D", name, cwd=workspace_dir)

    return {"status": "success", "message": f"Worktree '{name}' deleted"}


@router.get("/{name}/diff")
async def get_worktree_diff(name: str):
    worktree_path = os.path.join(_get_worktrees_base(), name)

    if not os.path.exists(worktree_path):
        raise HTTPException(status_code=404, detail=f"Worktree '{name}' not found")

    # Diff the worktree's working tree against its own HEAD
    stdout, stderr, rc = await call_git_command_with_output(
        "git", "diff", "HEAD", "--", ".", cwd=worktree_path
    )
    if rc != 0:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get diff: {stderr.strip()}",
        )

    return {"diff": stdout}


# --- Coding Agent container management ---

CODING_AGENT_IMAGE = os.environ.get(
    "BITSWAN_CODING_AGENT_IMAGE", "bitswan/coding-agent:latest"
)


@router.post("/coding-agent/ensure")
async def ensure_coding_agent():
    """Ensure the coding agent container is running. Start it if not."""
    workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace")
    agent_container_name = f"{workspace_name}-coding-agent"
    docker_client = get_async_docker_client()

    # Check if container already exists and is running
    try:
        containers = await docker_client.list_containers(
            all=True,
            filters={"name": [agent_container_name]},
        )
    except DockerError as e:
        raise HTTPException(status_code=500, detail=f"Docker error: {e}")

    for c in containers:
        names = c.get("Names", [])
        # Docker prefixes names with /
        if f"/{agent_container_name}" in names or agent_container_name in names:
            state = c.get("State", "")
            if state == "running":
                return {
                    "status": "running",
                    "message": "Coding agent is already running",
                }

            # Container exists but stopped — start it
            container_id = c.get("Id")
            try:
                await docker_client.start_container(container_id)
                return {
                    "status": "started",
                    "message": "Coding agent container started",
                }
            except DockerError as e:
                raise HTTPException(
                    status_code=500, detail=f"Failed to start agent container: {e}"
                )

    # Container doesn't exist — create and start it via Docker API
    bs_home = os.environ.get("BITSWAN_GITOPS_DIR", "/mnt/repo/pipeline")
    bs_home_host = os.environ.get("BITSWAN_GITOPS_DIR_HOST", bs_home)

    # Use the same secret the gitops server validates against.
    # If the env var isn't set, generate one and persist it in the environment
    # so verify_agent_token in agent.py can validate against it.
    agent_secret = os.environ.get("BITSWAN_GITOPS_AGENT_SECRET")
    if not agent_secret:
        agent_secret = uuid.uuid4().hex
        os.environ["BITSWAN_GITOPS_AGENT_SECRET"] = agent_secret

    network_name = "bitswan_network"

    # Read the editor's SSH public key to pass as env var
    # The SSH keys are mounted into the gitops container at /home/user1000/.ssh/
    ssh_pub_key = ""
    ssh_search_dirs = [
        "/home/user1000/.ssh",
        os.path.join(bs_home, "ssh"),
        os.path.join(bs_home, ".ssh"),
    ]
    for ssh_dir in ssh_search_dirs:
        for pub_name in ("id_ed25519.pub", "id_rsa.pub"):
            pub_path = os.path.join(ssh_dir, pub_name)
            if os.path.exists(pub_path):
                ssh_pub_key = open(pub_path).read().strip()
                break
        if ssh_pub_key:
            break

    # Volume binds must use host paths, not container paths
    bind_base = bs_home_host

    env_vars = [
        f"BITSWAN_GITOPS_URL=http://{workspace_name}-gitops:8079",
        f"BITSWAN_GITOPS_AGENT_SECRET={agent_secret}",
        f"BITSWAN_WORKSPACE_NAME={workspace_name}",
    ]
    if ssh_pub_key:
        env_vars.append(f"EDITOR_SSH_PUBLIC_KEY={ssh_pub_key}")

    container_config = {
        "Image": CODING_AGENT_IMAGE,
        "Hostname": agent_container_name,
        "Env": env_vars,
        "HostConfig": {
            "Binds": [
                f"{bind_base}/workspace/worktrees:/workspace/worktrees:z",
                f"{bind_base}/coding-agent-home:/home/agent:z",
                f"{bind_base}/coding-agent-sessions:/var/log/agent-sessions:z",
            ],
            "RestartPolicy": {"Name": "always"},
        },
        "NetworkingConfig": {
            "EndpointsConfig": {
                network_name: {},
            },
        },
    }

    try:
        # Pull image (best effort, timeout 60s)
        try:
            await docker_client._post(
                "/images/create",
                params={"fromImage": CODING_AGENT_IMAGE},
                timeout=60.0,
            )
        except Exception:
            logger.warning("Could not pull coding agent image, using local")

        # Create container
        resp = await docker_client._post(
            "/containers/create",
            params={"name": agent_container_name},
            json_data=container_config,
            timeout=30.0,
        )
        container_id = resp.get("Id")

        # Start container
        await docker_client.start_container(container_id)

        return {
            "status": "created",
            "message": "Coding agent container created and started",
        }
    except DockerError as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to create agent container: {e}"
        )
