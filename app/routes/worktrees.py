import asyncio
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
            logger.warning("Postgres dev container not found, skipping DB clone")
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


def _ensure_gitignore_patterns(gitignore_path: str, patterns: list[str]) -> bool:
    """Append any of `patterns` that aren't already listed in the .gitignore.

    Returns True when the file was modified, False otherwise. Best-effort:
    filesystem errors are swallowed so worktree creation isn't blocked by a
    flaky mount. Existing entries are kept; we only append.
    """
    try:
        existing = ""
        if os.path.exists(gitignore_path):
            with open(gitignore_path) as f:
                existing = f.read()
        existing_lines = {line.strip() for line in existing.splitlines()}
        to_add = [p for p in patterns if p not in existing_lines]
        if not to_add:
            return False
        with open(gitignore_path, "a") as f:
            if existing and not existing.endswith("\n"):
                f.write("\n")
            f.write("\n".join(to_add) + "\n")
        return True
    except OSError:
        return False


def _get_worktrees_base() -> str:
    return os.path.join(_get_workspace_dir(), "worktrees")


# Worktree names are used to construct filesystem paths AND are passed as
# positional args to git commands. The charset must rule out:
#   - path traversal (no `/`, no `.`, no `..`)
#   - leading `-` (git would parse it as an option)
#   - empty strings
_WORKTREE_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9\-]*$")


def _validate_worktree_name(name: str) -> None:
    if not name or not _WORKTREE_NAME_RE.match(name):
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid worktree name: must be alphanumeric with hyphens only "
                "and must not start with a hyphen."
            ),
        )


def _resolve_worktree_path(name: str) -> str:
    """Validate `name` and return the realpath to the worktree.

    Belt-and-suspenders against path traversal: even though the regex blocks
    `..`/`/`, we re-check via realpath so a malicious symlink inside the
    worktrees base can't redirect us elsewhere.
    """
    _validate_worktree_name(name)
    base = os.path.realpath(_get_worktrees_base())
    candidate = os.path.realpath(os.path.join(base, name))
    if candidate != base and not candidate.startswith(base + os.sep):
        raise HTTPException(status_code=400, detail="Invalid worktree name")
    return candidate


class CreateWorktreeRequest(BaseModel):
    branch_name: str
    base_branch: str = None  # auto-detect from current HEAD if not provided


@router.post("/create")
async def create_worktree(body: CreateWorktreeRequest):
    _validate_worktree_name(body.branch_name)

    workspace_dir = _get_workspace_dir()
    worktrees_base = _get_worktrees_base()
    worktree_path = os.path.join(worktrees_base, body.branch_name)

    if os.path.exists(worktree_path):
        raise HTTPException(
            status_code=409,
            detail=f"Worktree '{body.branch_name}' already exists",
        )

    # Ensure worktrees directory exists
    os.makedirs(worktrees_base, exist_ok=True)

    async with GitLockContext(timeout=15.0):
        # Ensure there is at least one commit — git worktree requires it
        _, _, rev_rc = await call_git_command_with_output(
            "git", "rev-parse", "HEAD", cwd=workspace_dir
        )
        if rev_rc != 0:
            # No commits yet — create an initial commit
            await call_git_command(
                "git",
                "commit",
                "--allow-empty",
                "-m",
                "initial commit",
                cwd=workspace_dir,
            )

        # Ensure the workspace .gitignore covers the worktrees tree and the
        # container-generated scratch files that shouldn't ever be committed.
        # Live-dev/automation containers create these inside the worktree and
        # they clutter `git status` (and block `git worktree remove` later).
        # Commit the update so the new worktree branches off a HEAD that
        # already has the ignores in place.
        gitignore_changed = _ensure_gitignore_patterns(
            os.path.join(workspace_dir, ".gitignore"),
            [
                "worktrees/",
                "__pycache__/",
                "*.pyc",
                "node_modules/",
                "node_modules",
                ".pytest_cache/",
                ".venv/",
                "dist/",
                "package.json",
                "!*/*/image/package.json",
                "config.js",
                "vite.config.live-dev.js",
            ],
        )
        if gitignore_changed:
            await call_git_command("git", "add", ".gitignore", cwd=workspace_dir)
            await call_git_command(
                "git",
                "commit",
                "-m",
                "gitops: ignore container-generated paths in worktrees",
                cwd=workspace_dir,
            )

        # Auto-detect base branch AFTER ensuring there is at least one commit.
        # Doing this before the initial commit would fail (no HEAD) and fall back
        # to "main", while git may have created the first commit on "master".
        base_branch = body.base_branch
        if not base_branch:
            stdout, _, rc = await call_git_command_with_output(
                "git", "rev-parse", "--abbrev-ref", "HEAD", cwd=workspace_dir
            )
            base_branch = stdout.strip() if rc == 0 and stdout.strip() else "main"

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

        # Check sync status: worktree is synced only when its branch tip
        # matches main AND there are no uncommitted changes.
        synced = False
        if branch:
            # Check main's HEAD is ancestor of worktree (worktree has main's changes)
            _, _, main_in_wt_rc = await call_git_command_with_output(
                "git",
                "merge-base",
                "--is-ancestor",
                "HEAD",
                branch,
                cwd=workspace_dir,
            )
            # Check worktree is ancestor of main (no commits ahead)
            _, _, wt_in_main_rc = await call_git_command_with_output(
                "git",
                "merge-base",
                "--is-ancestor",
                branch,
                "HEAD",
                cwd=workspace_dir,
            )
            # Check for uncommitted changes in the worktree
            diff_stdout, _, diff_rc = await call_git_command_with_output(
                "git",
                "status",
                "--porcelain",
                cwd=wt_path,
            )
            has_changes = bool(diff_stdout and diff_stdout.strip())
            synced = main_in_wt_rc == 0 and wt_in_main_rc == 0 and not has_changes

        result.append(
            {
                "name": name,
                "branch": branch,
                "commit_hash": commit_hash,
                "commit_message": commit_message,
                "has_requirements": has_requirements,
                "synced": synced,
            }
        )

    return result


class MergeWorktreeResponse(BaseModel):
    status: str
    message: str


@router.post("/{name}/merge")
async def merge_worktree(name: str):
    workspace_dir = _get_workspace_dir()
    worktree_path = _resolve_worktree_path(name)

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

        # Merge the worktree branch (validated above to be alnum + hyphens, no
        # leading hyphen, so it can't be parsed as a flag).
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


def _own_container_id_from_proc() -> str | None:
    """Scrape our own container ID from /proc entries.

    `$HOSTNAME` is unreliable — docker-compose commonly sets `hostname:` which
    overrides it, so it no longer matches the container name/ID.
    """
    # cgroup v1:       `12:pids:/docker/<id>`
    # cgroup v2+systemd: `0::/system.slice/docker-<id>.scope`
    # The `docker[-/]<id>` prefix disambiguates from other 64-hex strings.
    cgroup_re = re.compile(r"docker[-/]([0-9a-f]{64})")
    try:
        with open("/proc/self/cgroup") as f:
            for line in f:
                m = cgroup_re.search(line)
                if m:
                    return m.group(1)
    except OSError:
        pass
    # mountinfo: Docker always bind-mounts /etc/hostname, /etc/hosts and
    # /etc/resolv.conf from /var/lib/docker/containers/<id>/…, so the ID
    # appears as `/containers/<id>/` there. This discriminates against
    # overlay layer SHAs which use `/overlay2/<sha>/…`.
    try:
        with open("/proc/self/mountinfo") as f:
            for line in f:
                m = re.search(r"/containers/([0-9a-f]{64})/", line)
                if m:
                    return m.group(1)
    except OSError:
        pass
    return None


async def _own_container_id_from_api() -> str | None:
    """Ask the Docker daemon for a container whose hostname matches ours.

    Last-resort fallback when /proc parsing doesn't yield a usable ID (e.g.
    nested container runtimes, rootless Docker, unusual cgroup layout).
    """
    hostname = os.uname().nodename
    if not hostname:
        return None
    try:
        client = get_async_docker_client()
        containers = await client.list_containers(filters={"status": ["running"]})
        for c in containers:
            # Config lives under /json; the list endpoint exposes HostConfig but
            # not Config. Inspect each candidate — there shouldn't be many.
            cid = c.get("Id")
            if not cid:
                continue
            try:
                info = await client.get_container(cid)
                if info.get("Config", {}).get("Hostname") == hostname:
                    return cid
            except DockerError:
                continue
    except Exception as e:
        logger.debug("Docker API container lookup failed: %s", e)
    return None


async def _own_container_id() -> str | None:
    return _own_container_id_from_proc() or await _own_container_id_from_api()


async def _rm_rf_as_root_in_container(path: str) -> bool:
    """Wipe `path` as root by `docker exec`'ing into our own container.

    The gitops server runs as uid 1000, but worktree contents are populated by
    other containers (editor, live-dev automations, build outputs) that run as
    root or other uids. uid 1000 often can't unlink those files because the
    containing directories aren't writable by it. We have access to the Docker
    socket, so we re-enter our own container as root to do the removal.

    Returns True on success, False otherwise (caller should treat failure as
    hard error). Stderr is logged.
    """
    container_id = await _own_container_id()
    if not container_id:
        logger.warning(
            "rm -rf %s: could not determine own container ID "
            "(tried /proc/self/cgroup, /proc/self/mountinfo, Docker API by hostname); "
            "cannot docker exec as root",
            path,
        )
        return False
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker",
            "exec",
            "--user",
            "0",
            container_id,
            "rm",
            "-rf",
            path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.warning(
                "rm -rf %s via docker exec failed (%s): %s",
                path,
                proc.returncode,
                stderr.decode(errors="replace").strip(),
            )
            return False
        return True
    except Exception as e:
        logger.warning("rm -rf %s via docker exec raised: %s", path, e)
        return False


class CommitRequest(BaseModel):
    message: str
    worktree: str | None = None  # None = commit on the master workspace
    paths: list[str] | None = None  # None/empty = stage all changes (-A)


@router.post("/commit")
async def commit_changes(body: CommitRequest):
    """Stage and commit changes in either the master workspace or a worktree.

    Used by the editor UI to record file-system changes it just made
    (creating a business process, copying an automation template, etc.) so
    they don't sit as untracked files indefinitely.
    """
    if body.worktree:
        repo_path = _resolve_worktree_path(body.worktree)
        if not os.path.exists(repo_path):
            raise HTTPException(
                status_code=404, detail=f"Worktree '{body.worktree}' not found"
            )
    else:
        repo_path = _get_workspace_dir()

    # Reject path traversal / absolute paths in user-supplied paths.
    safe_paths: list[str] | None = None
    if body.paths:
        for p in body.paths:
            if os.path.isabs(p) or any(part == ".." for part in p.split(os.sep)):
                raise HTTPException(status_code=400, detail=f"Invalid path: {p}")
        safe_paths = body.paths

    async with GitLockContext(timeout=15.0):
        if safe_paths:
            success = await call_git_command(
                "git", "add", "--", *safe_paths, cwd=repo_path
            )
        else:
            success = await call_git_command("git", "add", "-A", cwd=repo_path)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to stage changes")

        stdout, stderr, rc = await call_git_command_with_output(
            "git", "commit", "-m", body.message, cwd=repo_path
        )
        if rc != 0:
            combined = (stdout or "") + (stderr or "")
            if "nothing to commit" in combined:
                return {"status": "noop", "message": "Nothing to commit"}
            raise HTTPException(
                status_code=500,
                detail=f"Failed to commit: {(stderr or stdout).strip()}",
            )

    hash_stdout, _, hash_rc = await call_git_command_with_output(
        "git", "rev-parse", "HEAD", cwd=repo_path
    )
    return {
        "status": "success",
        "commit_hash": hash_stdout.strip() if hash_rc == 0 else "unknown",
    }


@router.delete("/{name}")
async def delete_worktree(name: str):
    workspace_dir = _get_workspace_dir()
    worktree_path = _resolve_worktree_path(name)

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
            # Fallback: worktree contents often include files owned by other
            # uids (live-dev containers, editor, build artifacts) that user1000
            # can't unlink. Wipe the tree as root via docker exec, then prune
            # the git metadata that `worktree remove` normally maintains.
            logger.info(
                "`git worktree remove` failed for '%s'; retrying with privileged rm -rf",
                name,
            )
            if not await _rm_rf_as_root_in_container(worktree_path):
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to remove worktree '{name}'",
                )
            await call_git_command("git", "worktree", "prune", cwd=workspace_dir)

        # Delete the branch
        await call_git_command("git", "branch", "-D", name, cwd=workspace_dir)

    return {"status": "success", "message": f"Worktree '{name}' deleted"}


@router.get("/{name}/diff")
async def get_worktree_diff(name: str):
    worktree_path = _resolve_worktree_path(name)

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


class EnsureCodingAgentRequest(BaseModel):
    image: str | None = None
    dev_mode: bool = False
    source_dir: str | None = None  # host path to bitswan-agent source


@router.post("/coding-agent/ensure")
async def ensure_coding_agent(body: EnsureCodingAgentRequest | None = None):
    """Ensure the coding agent container is running. Start it if not."""
    custom_image = body.image if body else None
    dev_mode = body.dev_mode if body else False
    source_dir = body.source_dir if body else None
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
        if f"/{agent_container_name}" in names or agent_container_name in names:
            # If custom image or dev mode requested, recreate the container
            if custom_image or dev_mode:
                container_id = c.get("Id")
                try:
                    await docker_client.stop_container(container_id, timeout=5)
                except Exception:
                    pass
                try:
                    await docker_client.remove_container(container_id, force=True)
                except Exception:
                    pass
                break  # fall through to create

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

    image = custom_image or CODING_AGENT_IMAGE
    binds = [
        f"{bind_base}/workspace/worktrees:/workspace/worktrees:z",
        f"{bind_base}/coding-agent-home:/home/agent:z",
        f"{bind_base}/coding-agent-sessions:/var/log/agent-sessions:z",
    ]
    if dev_mode and source_dir:
        binds.append(
            f"{source_dir}/agent-session-wrapper:/usr/local/bin/agent-session-wrapper:z"
        )
        binds.append(f"{source_dir}/AGENTS-inside-container.md:/AGENTS.md:z")

    container_config = {
        "Image": image,
        "Hostname": agent_container_name,
        "Env": env_vars,
        "HostConfig": {
            "Binds": binds,
            "RestartPolicy": {"Name": "always"},
        },
        "NetworkingConfig": {
            "EndpointsConfig": {
                network_name: {},
            },
        },
    }

    try:
        # Pull image (best effort, skip for custom local images)
        if not custom_image:
            try:
                await docker_client._post(
                    "/images/create",
                    params={"fromImage": image},
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
