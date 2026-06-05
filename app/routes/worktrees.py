import asyncio
import logging
import os
import re

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.async_docker import get_async_docker_client, DockerError
from app.deploy_runner import spawn_set_deploy
from app.services.automation_service import scan_workspace_sources
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


# Allowed character set for arbitrary git refs (branches, tags). Stricter
# than `git check-ref-format` but accepts every legitimate name we expect
# (e.g. `main`, `feature/foo`, `v1.0`).
_REF_NAME_RE = re.compile(r"^[A-Za-z0-9._/\-]+$")


def _validate_ref_name(name: str) -> None:
    """Validate `name` looks like a safe git ref before passing it to git.

    Defends against option injection (leading `-`), shell metacharacters,
    and the common git ref pitfalls listed in `git check-ref-format(1)`:
    `..`, `@{`, leading/trailing `/` or `.`, `.lock` suffix, `//`. The
    `_validate_worktree_name` regex is too strict for refs (which may
    contain `/` and `.`), so this is a separate, slightly looser check.
    """
    if (
        not name
        or name.startswith("-")
        or not _REF_NAME_RE.match(name)
        or ".." in name
        or "@{" in name
        or name.startswith(("/", "."))
        or name.endswith(("/", ".", ".lock"))
        or "//" in name
    ):
        raise HTTPException(status_code=400, detail="Invalid ref name")


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
                ".pytest_cache/",
                ".venv/",
                "dist/",
            ],
        )
        if gitignore_changed:
            # Pin the commit to the .gitignore pathspec so any unrelated
            # already-staged changes in the index don't get swept into this
            # gitops-managed commit. With a pathspec, `git commit` records
            # only those paths (akin to --only), not the whole index.
            await call_git_command(
                "git",
                "commit",
                "-m",
                "gitops: ignore container-generated paths in worktrees",
                "--",
                ".gitignore",
                cwd=workspace_dir,
            )

        # Auto-detect base branch AFTER ensuring there is at least one commit.
        # Doing this before the initial commit would fail (no HEAD) and fall back
        # to "main", while git may have created the first commit on "master".
        # Validate any client-supplied base_branch up front so we never hand
        # something like `-D` (or any other option-shaped value) to `git branch`.
        # Auto-detected refs come from `git rev-parse` and are already trusted.
        if body.base_branch:
            _validate_ref_name(body.base_branch)
            base_branch = body.base_branch
        else:
            stdout, _, rc = await call_git_command_with_output(
                "git", "rev-parse", "--abbrev-ref", "HEAD", cwd=workspace_dir
            )
            base_branch = stdout.strip() if rc == 0 and stdout.strip() else "main"

        # Create the branch from base. `--` ends option processing as a
        # belt-and-suspenders against any future arg that slips by validation.
        success = await call_git_command(
            "git", "branch", "--", body.branch_name, base_branch, cwd=workspace_dir
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

    # Auto-start live-dev for every automation in the new worktree (a fresh
    # worktree carries all of main's BPs). Best-effort — the worktree was
    # already created; failures are logged + reported via `deploy_error`.
    try:
        members = scan_workspace_sources(
            _get_workspace_dir(), worktree=body.branch_name
        )
        res = await spawn_set_deploy(
            label=f"wt:{body.branch_name}",
            members=members,
            stage="live-dev",
            worktree=body.branch_name,
        )
        if res.get("deploy"):
            result["deploy_task_id"] = res["deploy"]["task_id"]
        elif res.get("error"):
            result["deploy_error"] = res["error"]
    except Exception as e:
        logger.warning(
            "Worktree auto-deploy spawn failed for '%s': %s", body.branch_name, e
        )
        result["deploy_error"] = str(e)

    return result


# In-memory cache for the worktree list. Refreshed by the filesystem watcher
# in `lifespan.py` whenever something under `worktrees/` changes, and
# broadcast over the `/events/stream` SSE feed as a `worktrees` event so the
# dashboard never has to poll. `None` means "never computed yet" — the route
# will compute on demand.
_worktrees_cache: list[dict] | None = None


async def _compute_worktrees() -> list[dict]:
    """Heavy work: shell out to git (per-worktree) and assemble the listing.

    Same data the `GET /worktrees/` route used to return inline; lifted to a
    function so both the route and the watcher-driven cache refresh share
    the implementation.
    """
    workspace_dir = _get_workspace_dir()
    worktrees_base = _get_worktrees_base()

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


async def get_cached_worktrees() -> list[dict]:
    """Return the cached worktree list, computing on first call."""
    global _worktrees_cache
    if _worktrees_cache is None:
        _worktrees_cache = await _compute_worktrees()
    return _worktrees_cache


async def refresh_worktrees() -> list[dict]:
    """Re-run the worktree scan and update the cache. Used by the filesystem
    watcher whenever something under `worktrees/` changes."""
    global _worktrees_cache
    _worktrees_cache = await _compute_worktrees()
    return _worktrees_cache


@router.get("/")
async def list_worktrees():
    return await get_cached_worktrees()


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


def _is_safe_relative_path(p: str) -> bool:
    """True if `p` looks like a workspace-relative path (no `..`, no leading `/`)."""
    if not p:
        return False
    if p.startswith("/") or p.startswith("\\"):
        return False
    parts = re.split(r"[\\/]", p)
    return not any(seg in ("", "..") for seg in parts)


# Map a porcelain status code to the simple A/M/D taxonomy the dashboard
# renders. Renames collapse to M for now — the design only knows A/M/D.
def _porcelain_to_kind(code: str) -> str | None:
    """`code` is the two-char xy prefix from `git status --porcelain`."""
    if not code:
        return None
    x, y = code[0], code[1] if len(code) > 1 else " "
    # Untracked file (??)
    if x == "?" and y == "?":
        return "A"
    # Deleted on either side
    if x == "D" or y == "D":
        return "D"
    # Added on either side
    if x == "A" or y == "A":
        return "A"
    # Renamed collapses to M for v1
    if x == "R" or y == "R":
        return "M"
    # Anything else (modified, copied, type-changed) → M
    if x == "M" or y == "M" or x == "T" or y == "T" or x == "C" or y == "C":
        return "M"
    return None


@router.get("/{name}/status")
async def get_worktree_status(name: str):
    """Per-file change list for a worktree.

    Combines `git status --porcelain=v1 -z` (which files changed and how)
    with `git diff --numstat HEAD` (how many lines added / removed). Used
    by the dashboard's Diff / Files tabs.
    """
    worktree_path = _resolve_worktree_path(name)
    if not os.path.exists(worktree_path):
        raise HTTPException(status_code=404, detail=f"Worktree '{name}' not found")

    # `--porcelain=v1 -z` uses NUL terminators between records and never
    # quotes file paths, so we don't need to deal with shell-quoted names.
    status_out, status_err, status_rc = await call_git_command_with_output(
        "git",
        "status",
        "--porcelain=v1",
        "-z",
        cwd=worktree_path,
    )
    if status_rc != 0:
        raise HTTPException(
            status_code=500, detail=f"git status failed: {status_err.strip()}"
        )

    changed: list[dict] = []
    # Records are NUL-separated; rename entries are two records (new\0old).
    records = status_out.split("\x00")
    skip_next = False
    for rec in records:
        if skip_next:
            skip_next = False
            continue
        if not rec:
            continue
        # Format: "XY path"  (XY is two columns)
        code = rec[:2]
        path = rec[3:] if len(rec) > 3 else ""
        kind = _porcelain_to_kind(code)
        if not kind or not path:
            continue
        # Rename: next NUL-delimited entry is the OLD name; skip it.
        if code[0] == "R" or code[1] == "R":
            skip_next = True
        changed.append({"path": path, "kind": kind, "adds": 0, "dels": 0})

    # Merge in adds/dels from `git diff --numstat HEAD`. Untracked files
    # don't show up here (diff is against HEAD); leave their counts at 0
    # — the user can see exact contents in the Files tab.
    numstat_out, _, numstat_rc = await call_git_command_with_output(
        "git", "diff", "--numstat", "HEAD", cwd=worktree_path
    )
    if numstat_rc == 0:
        # Lines are `adds\tdels\tpath`. Binary files use `-\t-`.
        by_path = {c["path"]: c for c in changed}
        for line in numstat_out.splitlines():
            parts = line.split("\t", 2)
            if len(parts) != 3:
                continue
            adds_str, dels_str, path = parts
            adds = int(adds_str) if adds_str.isdigit() else 0
            dels = int(dels_str) if dels_str.isdigit() else 0
            entry = by_path.get(path)
            if entry is not None:
                entry["adds"] = adds
                entry["dels"] = dels

    return {"changed": changed}


@router.get("/{name}/diff")
async def get_worktree_diff(
    name: str,
    path: str | None = Query(None),
):
    """Unified diff of the worktree against its own HEAD. Optional `?path=`
    filters to a single workspace-relative file."""
    worktree_path = _resolve_worktree_path(name)

    if not os.path.exists(worktree_path):
        raise HTTPException(status_code=404, detail=f"Worktree '{name}' not found")

    git_args: list[str] = ["git", "diff", "HEAD"]
    if path is not None:
        if not _is_safe_relative_path(path):
            raise HTTPException(status_code=400, detail="invalid path")
        git_args += ["--", path]
    else:
        git_args += ["--", "."]

    stdout, stderr, rc = await call_git_command_with_output(
        *git_args, cwd=worktree_path
    )
    if rc != 0:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get diff: {stderr.strip()}",
        )

    return {"diff": stdout}
