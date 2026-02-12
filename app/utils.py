import asyncio
from configparser import ConfigParser
from dataclasses import dataclass
from io import StringIO
from datetime import datetime, timezone
import hashlib
import os
import threading
from typing import Any, Optional
import shlex
import subprocess
import shutil
import tempfile

import humanize
import toml
import yaml
import requests
from fastapi import HTTPException


# Thread-safe git lock that works across both async and sync contexts
# Uses a threading.Lock as the underlying mechanism for cross-thread safety
_git_thread_lock = threading.Lock()


class GitLockContext:
    """
    Context manager for git lock that works in both async and sync contexts.
    Uses a threading.Lock internally for cross-thread safety (needed for background threads).
    """

    def __init__(self, timeout: float = 10.0):
        self.timeout = timeout
        self._acquired = False

    async def __aenter__(self):
        """Async context manager entry - acquires lock without blocking event loop."""
        loop = asyncio.get_event_loop()
        # Run the blocking lock acquisition in a thread pool to avoid blocking the event loop
        acquired = await loop.run_in_executor(
            None, lambda: _git_thread_lock.acquire(timeout=self.timeout)
        )
        if not acquired:
            raise Exception(f"Failed to acquire git lock within {self.timeout} seconds")
        self._acquired = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - releases lock."""
        if self._acquired:
            _git_thread_lock.release()
            self._acquired = False
        return False

    def __enter__(self):
        """Sync context manager entry - for use in background threads."""
        acquired = _git_thread_lock.acquire(timeout=self.timeout)
        if not acquired:
            raise Exception(f"Failed to acquire git lock within {self.timeout} seconds")
        self._acquired = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Sync context manager exit - for use in background threads."""
        if self._acquired:
            _git_thread_lock.release()
            self._acquired = False
        return False


@dataclass
class ServiceDependency:
    """A declared infrastructure service dependency from automation.toml."""

    enabled: bool = True


KNOWN_STAGES = {"live-dev", "dev", "staging", "production"}

# Network realms: each gets its own set of infrastructure services (Kafka, CouchDB).
# live-dev shares the dev realm.
SERVICE_REALMS = {"dev", "staging", "production"}


@dataclass
class AutomationConfig:
    """Unified automation configuration from either automation.toml or pipelines.conf."""

    id: str | None = (
        None  # Unique automation ID (used as Keycloak client_id when auth=True)
    )
    auth: bool = False  # Enable Keycloak authentication
    image: str = "bitswan/pipeline-runtime-environment:latest"
    expose: bool = False
    expose_to: list[str] | None = None
    port: int = 8080
    config_format: str = "ini"  # "toml" or "ini"
    mount_path: str = "/opt/pipelines"  # "/app/" for TOML, "/opt/pipelines" for INI
    # Stage-specific secret groups (only for automation.toml - no general fallback)
    live_dev_groups: list[str] | None = None
    dev_groups: list[str] | None = None
    staging_groups: list[str] | None = None
    production_groups: list[str] | None = None
    # CORS allowed domains for Keycloak client (optional)
    allowed_domains: list[str] | None = None
    # Infrastructure service dependencies
    services: dict[str, ServiceDependency] | None = None


def _parse_string_or_list(value) -> list[str] | None:
    """Parse a value that can be either a string or list into a list."""
    if value is None:
        return None
    if isinstance(value, list):
        return [str(g).strip() for g in value if str(g).strip()]
    if isinstance(value, str):
        return [g.strip() for g in value.split() if g.strip()]
    return None


def parse_automation_toml(content: str) -> AutomationConfig | None:
    """Parse automation.toml content from a string and return AutomationConfig."""
    if not content or not content.strip():
        return None
    try:
        data = toml.loads(content)
        deployment = data.get("deployment", {})
        secrets = data.get("secrets", {})

        # Parse expose_to as a list
        expose_to = deployment.get("expose_to")
        if isinstance(expose_to, str):
            expose_to = [g.strip() for g in expose_to.split(",") if g.strip()]

        # Parse allowed_domains as a list (for CORS in Keycloak client)
        allowed_domains = deployment.get("allowed_domains")
        if isinstance(allowed_domains, list):
            allowed_domains = [
                str(d).strip() for d in allowed_domains if str(d).strip()
            ]
        else:
            allowed_domains = None

        # Parse [services.*] sections
        services_data = data.get("services", {})
        services = None
        if services_data and isinstance(services_data, dict):
            services = {}
            for svc_type, svc_conf in services_data.items():
                if not isinstance(svc_conf, dict):
                    continue
                services[svc_type] = ServiceDependency(
                    enabled=svc_conf.get("enabled", True),
                )

        return AutomationConfig(
            id=deployment.get("id"),
            auth=deployment.get("auth", False),
            image=deployment.get(
                "image", "bitswan/pipeline-runtime-environment:latest"
            ),
            expose=deployment.get("expose", False),
            expose_to=expose_to,
            port=deployment.get("port", 8080),
            config_format="toml",
            mount_path="/app/",
            live_dev_groups=_parse_string_or_list(secrets.get("live-dev")),
            dev_groups=_parse_string_or_list(secrets.get("dev")),
            staging_groups=_parse_string_or_list(secrets.get("staging")),
            production_groups=_parse_string_or_list(secrets.get("production")),
            allowed_domains=allowed_domains,
            services=services,
        )
    except Exception:
        return None


def read_automation_toml(source_dir: str) -> AutomationConfig | None:
    """Read automation.toml from a directory."""
    toml_path = os.path.join(source_dir, "automation.toml")
    if os.path.exists(toml_path):
        with open(toml_path, "r") as f:
            content = f.read()
        return parse_automation_toml(content)
    return None


def read_automation_config(source_dir: str) -> AutomationConfig:
    """
    Read automation configuration with priority: automation.toml > pipelines.conf.
    Returns AutomationConfig with deployment settings.
    """
    # Try automation.toml first (highest priority)
    toml_config = read_automation_toml(source_dir)
    if toml_config:
        return toml_config

    # Fall back to pipelines.conf
    pipeline_conf = read_pipeline_conf(source_dir)
    if pipeline_conf:
        # Parse expose_to as a list
        expose_to = None
        if pipeline_conf.has_option("deployment", "expose_to"):
            expose_to_value = pipeline_conf.get("deployment", "expose_to")
            expose_to = [g.strip() for g in expose_to_value.split(",") if g.strip()]

        # Parse id and auth for Keycloak
        automation_id = None
        if pipeline_conf.has_option("deployment", "id"):
            automation_id = pipeline_conf.get("deployment", "id")
        auth = pipeline_conf.getboolean("deployment", "auth", fallback=False)

        return AutomationConfig(
            id=automation_id,
            auth=auth,
            image=pipeline_conf.get(
                "deployment",
                "pre",
                fallback="bitswan/pipeline-runtime-environment:latest",
            ),
            expose=pipeline_conf.getboolean("deployment", "expose", fallback=False),
            expose_to=expose_to,
            port=int(pipeline_conf.get("deployment", "port", fallback="8080")),
            config_format="ini",
            mount_path="/opt/pipelines",
        )

    # Return default config if no config file found
    return AutomationConfig()


async def wait_coroutine(*args, **kwargs) -> int:
    coro = await asyncio.create_subprocess_exec(*args, **kwargs)
    result = await coro.wait()
    return result


def _build_git_command(*command, cwd=None):
    """
    Build the command to execute, handling HOST_PATH case with nsenter if needed.
    Returns (exec_command, proc_kwargs) where exec_command is the command list
    and proc_kwargs are kwargs for subprocess execution.
    """
    host_path = os.environ.get("HOST_PATH")
    host_home = os.environ.get("HOST_HOME")
    host_user = os.environ.get("HOST_USER")

    # If all host environment variables are set, use nsenter to run git command on host
    if cwd and host_path and host_home and host_user:
        formatted_command = " ".join(shlex.quote(arg) for arg in command)
        host_command = (
            f"PATH={host_path} su - {host_user} -c "
            f'"cd {cwd} && PATH={host_path} HOME={host_home} {formatted_command}"'
        )
        exec_command = [
            "nsenter",
            "-t",
            "1",
            "-m",
            "-u",
            "-n",
            "-i",
            "sh",
            "-c",
            host_command,
        ]
        return exec_command, {}
    else:
        # Fallback to local git command
        return list(command), {"cwd": cwd}


async def call_git_command(*command, **kwargs) -> bool:
    cwd = kwargs.get("cwd")
    exec_command, proc_kwargs = _build_git_command(*command, cwd=cwd)
    result = await wait_coroutine(*exec_command, **proc_kwargs)
    return result == 0


async def call_git_command_with_output(*command, **kwargs) -> tuple[str, str, int]:
    """
    Execute a git command and return (stdout, stderr, return_code).
    Handles HOST_PATH case using nsenter if needed.
    """
    cwd = kwargs.get("cwd")
    exec_command, proc_kwargs = _build_git_command(*command, cwd=cwd)

    # Execute the command and capture output
    proc = await asyncio.create_subprocess_exec(
        *exec_command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        **proc_kwargs,
    )
    stdout, stderr = await proc.communicate()
    return stdout.decode(), stderr.decode(), proc.returncode


def read_bitswan_yaml(bitswan_dir: str) -> dict[str, Any] | None:
    bitswan_yaml_path = os.path.join(bitswan_dir, "bitswan.yaml")
    try:
        if os.path.exists(bitswan_yaml_path):
            with open(bitswan_yaml_path, "r") as f:
                bs_yaml: dict = yaml.safe_load(f)
                return bs_yaml
    except Exception:
        return None


def calculate_uptime(created_at: str) -> str:
    created_at = datetime.fromisoformat(created_at)
    uptime = datetime.now(timezone.utc) - created_at
    return humanize.naturaldelta(uptime)


def parse_pipeline_conf(content: str) -> ConfigParser | None:
    """Parse pipelines.conf content from a string."""
    if not content or not content.strip():
        return None
    try:
        config = ConfigParser()
        config.read_file(StringIO(content))
        return config
    except Exception:
        return None


def read_pipeline_conf(source_dir: str) -> ConfigParser | None:
    """Read pipelines.conf from a directory (legacy method, uses parse_pipeline_conf internally)."""
    conf_file_path = os.path.join(source_dir, "pipelines.conf")
    if os.path.exists(conf_file_path):
        with open(conf_file_path, "r") as f:
            content = f.read()
        return parse_pipeline_conf(content)
    return None


def test_read_pipeline_conf():
    import tempfile

    # create a tempdir with a pipelines.conf file
    with tempfile.TemporaryDirectory() as tmpdirname:
        with open(os.path.join(tmpdirname, "pipelines.conf"), "w") as f:
            f.write("[pipeline1]\n")
            f.write("key1=value1\n")
            f.write("key2=value2\n")

        config = read_pipeline_conf(tmpdirname)
        assert config.get("pipeline1", "key1") == "value1"


def generate_workspace_url(workspace_name, deployment_id, gitops_domain, full=False):
    url = "{}-{}.{}".format(workspace_name, deployment_id, gitops_domain)
    return f"https://{url}" if full else url


def add_workspace_route_to_caddy(deployment_id: str, port: str) -> bool:
    gitops_domain = os.environ.get("BITSWAN_GITOPS_DOMAIN", "gitops.bitswan.space")
    workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace-local")
    endpoint = generate_workspace_url(
        workspace_name, deployment_id, gitops_domain, False
    )

    caddy_id = get_workspace_caddy_id(deployment_id, workspace_name)
    dial_address = f"{workspace_name}__{deployment_id}:{port}"

    return add_route_to_caddy(endpoint, caddy_id, dial_address)


def add_route_to_caddy(route: str, caddy_id: str, dial_address: str) -> bool:
    caddy_url = os.environ.get("CADDY_URL", "http://caddy:2019")
    upstreams = requests.get(f"{caddy_url}/reverse_proxy/upstreams")

    if upstreams.status_code != 200:
        return False

    upstreams = upstreams.json()
    for upstream in upstreams:
        # deployment_id is already in the upstreams
        if upstream.get("address") == dial_address:
            return True

    body = [
        {
            "@id": caddy_id,
            "match": [{"host": [route]}],
            "handle": [
                {
                    "handler": "subroute",
                    "routes": [
                        {
                            "handle": [
                                {
                                    "handler": "reverse_proxy",
                                    "upstreams": [{"dial": dial_address}],
                                }
                            ]
                        }
                    ],
                }
            ],
            "terminal": True,
        }
    ]

    routes_url = "{}/config/apps/http/servers/srv0/routes/...".format(caddy_url)
    response = requests.post(routes_url, json=body)
    return response.status_code == 200


def get_workspace_caddy_id(deployment_id, workspace_name):
    return "{}.{}".format(deployment_id, workspace_name)


def remove_route_from_caddy(deployment_id: str, workspace_name: str):
    caddy_url = os.environ.get("CADDY_URL", "http://caddy:2019")
    routes_url = "{}/id/{}".format(
        caddy_url, get_workspace_caddy_id(deployment_id, workspace_name)
    )
    response = requests.delete(routes_url)
    return response.status_code == 200


def calculate_checksum(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def _calculate_git_blob_hash(file_path: str) -> str:
    """
    Calculate git blob hash for a file (SHA1 of "blob <size>\\0<content>").
    """
    with open(file_path, "rb") as f:
        content = f.read()

    size = len(content)
    header = f"blob {size}\0".encode("utf-8")
    blob = header + content
    return hashlib.sha1(blob).hexdigest()


def _calculate_git_tree_hash_recursive(
    dir_path: str, relative_path: str = "", logger=None
) -> str:
    """
    Calculate git tree hash for a directory recursively.
    Implements git's tree object format directly without spawning git processes.
    Tree format: "tree <size>\\0<entries>" where each entry is "<mode> <name>\\0<20-byte-sha1>"
    """
    entries = []

    # Get all entries and sort them (directories first, then alphabetical)
    items = []
    for item in os.listdir(dir_path):
        if item == ".git":
            continue
        item_path = os.path.join(dir_path, item)
        # Skip symlinks - they should not be included in deployments
        if os.path.islink(item_path):
            if logger:
                entry_relative_path = (
                    f"{relative_path}/{item}" if relative_path else item
                )
                logger.info(f"Skipping symlink: {entry_relative_path}")
            continue
        is_dir = os.path.isdir(item_path)
        # Skip anything that's not a regular file or directory
        if not is_dir and not os.path.isfile(item_path):
            continue
        items.append((item, is_dir))

    def _git_sort_key(name: str, is_dir: bool) -> bytes:
        key = f"{name}/" if is_dir else name
        return key.encode("utf-8")

    # Git-style ordering
    items.sort(key=lambda x: _git_sort_key(x[0], x[1]))

    for name, is_dir in items:
        item_path = os.path.join(dir_path, name)
        entry_relative_path = f"{relative_path}/{name}" if relative_path else name

        if is_dir:
            tree_hash = _calculate_git_tree_hash_recursive(
                item_path, entry_relative_path, logger
            )
            entries.append({"mode": "040000", "name": name, "hash": tree_hash})
            if logger:
                logger.info(f"CHECKSUM DIR:  {entry_relative_path}/ -> {tree_hash}")
        else:
            # Always use 100644 mode - zip extraction doesn't preserve executable bits reliably
            blob_hash = _calculate_git_blob_hash(item_path)
            entries.append({"mode": "100644", "name": name, "hash": blob_hash})
            if logger:
                logger.info(
                    f"CHECKSUM FILE: {entry_relative_path} -> 100644 {blob_hash}"
                )

    # Build tree object: "tree <size>\\0<entries>"
    entry_bytes = bytearray()
    for entry in entries:
        # Each entry: "<mode> <name>\\0<20-byte-sha1>"
        hash_bytes = bytes.fromhex(entry["hash"])
        entry_str = f"{entry['mode']} {entry['name']}\0"
        entry_bytes.extend(entry_str.encode("utf-8"))
        entry_bytes.extend(hash_bytes)

    tree_content = bytes(entry_bytes)
    tree_header = f"tree {len(tree_content)}\0".encode("utf-8")
    result_hash = hashlib.sha1(tree_header + tree_content).hexdigest()
    return result_hash


async def calculate_git_tree_hash(dir_path: str) -> str:
    """
    Calculate git tree hash for a directory using git's tree object format.
    This implementation directly calculates the hash without spawning git processes,
    making it much more efficient.
    """
    import logging

    logger = logging.getLogger(__name__)
    logger.info(f"=== SERVER CHECKSUM CALCULATION START for {dir_path} ===")

    # Run the recursive calculation in a thread pool to keep it async
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None, lambda: _calculate_git_tree_hash_recursive(dir_path, "", logger)
    )

    logger.info(f"=== SERVER CHECKSUM CALCULATION END: {result} ===")
    return result


async def update_git(
    bitswan_home: str, bitswan_home_host: str, deployment_id: str, action: str
):
    """
    Update git repository with changes to bitswan.yaml.

    Uses async lock with minimal hold time - only during the actual git operations
    that need to be atomic (add, commit). Pull and push are done with retries
    to handle concurrent access gracefully.
    """
    host_path = os.environ.get("HOST_PATH")

    if host_path:
        bitswan_dir = bitswan_home_host
    else:
        bitswan_dir = bitswan_home

    bitswan_yaml_path = os.path.join(bitswan_dir, "bitswan.yaml")

    # Check if we have a remote (this is a read-only operation, no lock needed)
    has_remote = await call_git_command(
        "git", "remote", "show", "origin", cwd=bitswan_dir
    )

    # Use async lock with shorter timeout - operations should be fast
    async with GitLockContext(timeout=10.0):
        # Pull latest changes if we have a remote
        if has_remote:
            res = await call_git_command(
                "git", "pull", "--rebase=false", cwd=bitswan_dir
            )
            if not res:
                # Try to recover from merge conflicts by accepting ours for bitswan.yaml
                await call_git_command(
                    "git", "checkout", "--ours", bitswan_yaml_path, cwd=bitswan_dir
                )
                await call_git_command("git", "add", bitswan_yaml_path, cwd=bitswan_dir)

        # Stage and commit changes
        await call_git_command("git", "add", bitswan_yaml_path, cwd=bitswan_dir)

        # Also stage docker-compose.yaml if it exists (generated by AutomationService)
        # Check existence using the container path (bitswan_home), but add using bitswan_dir
        # which may be the host path when HOST_PATH is set.
        dc_container_path = os.path.join(bitswan_home, "docker-compose.yaml")
        if os.path.exists(dc_container_path):
            dc_git_path = os.path.join(bitswan_dir, "docker-compose.yaml")
            await call_git_command("git", "add", dc_git_path, cwd=bitswan_dir)

        await call_git_command(
            "git",
            "commit",
            "--author",
            "gitops <info@bitswan.space>",
            "-m",
            f"{action} deployment {deployment_id}",
            cwd=bitswan_dir,
        )

        subprocess.run(["chown", "-R", "1000:1000", "/gitops/gitops"], check=False)

        # Push changes if we have a remote
        if has_remote:
            res = await call_git_command("git", "push", cwd=bitswan_dir)
            if not res:
                raise Exception("Error pushing to git")


async def docker_compose_up(
    bitswan_dir: str,
    docker_compose: str,
    container_name: str | None = None,
    extra_services: list[str] | None = None,
) -> None:
    async def setup_asyncio_process(cmd: list[Any]) -> dict[str, Any]:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=bitswan_dir,
        )

        stdout, stderr = await proc.communicate(input=docker_compose.encode())

        res = {
            "cmd": cmd,
            "stdout": stdout.decode("utf-8"),
            "stderr": stderr.decode("utf-8"),
            "return_code": proc.returncode,
        }
        return res

    docker_compose_cmd = [
        "docker",
        "compose",
        "-f",
        "/dev/stdin",
        "up",
        "-d",
        "--remove-orphans",
    ]
    if container_name:
        docker_compose_cmd.append(container_name)
    if extra_services:
        docker_compose_cmd.extend(extra_services)

    up_result = await setup_asyncio_process(docker_compose_cmd)

    return {
        "up_result": up_result,
    }


async def save_image(
    build_context_path: str,
    build_context_hash: str,
    image_tag: str,
    copy_context: bool = True,
    build_status: Optional[str] = None,
    log_file_path: Optional[str] = None,
):
    """
    Save and commit the build context (extracted zip contents) to git.
    Uses the build_context_hash as the directory name for deduplication.

    Uses async lock with minimal hold time for better concurrency.
    """

    bs_home = os.environ.get("BITSWAN_GITOPS_DIR", "/mnt/repo/pipeline")
    bitswan_dir = os.path.join(bs_home, "gitops")

    images_base_dir = os.path.join(bitswan_dir, "images")
    image_dir = os.path.join(images_base_dir, build_context_hash)
    source_dir = os.path.join(image_dir, "src")

    # File system operations don't need the git lock
    if not os.path.exists(images_base_dir):
        os.makedirs(images_base_dir, exist_ok=True)
        subprocess.run(["chown", "-R", "1000:1000", images_base_dir], check=False)

    # Create the specific image directory
    os.makedirs(image_dir, exist_ok=True)
    subprocess.run(["chown", "-R", "1000:1000", image_dir], check=False)

    # Copy the build context to the gitops directory if requested
    if copy_context:
        source_abs = os.path.abspath(build_context_path)
        destination_abs = os.path.abspath(source_dir)
        if source_abs != destination_abs:
            if os.path.exists(source_dir):
                shutil.rmtree(source_dir)
            shutil.copytree(build_context_path, source_dir)

    log_relative_path = None
    if log_file_path and os.path.exists(log_file_path):
        try:
            log_relative_path = os.path.relpath(log_file_path, bitswan_dir)
        except ValueError:
            log_relative_path = None

    # Check if we have a remote (read-only, no lock needed)
    has_remote = await call_git_command(
        "git", "remote", "show", "origin", cwd=bitswan_dir
    )

    # Use async lock for the actual git operations
    async with GitLockContext(timeout=10.0):
        if has_remote:
            res = await call_git_command(
                "git", "pull", "--rebase=false", cwd=bitswan_dir
            )
            if not res:
                # Non-fatal - we'll try to push anyway
                pass

        add_result = await call_git_command(
            "git", "add", os.path.join("images", build_context_hash), cwd=bitswan_dir
        )
        if not add_result:
            raise Exception("Error adding files to git")

        if log_relative_path:
            log_add_result = await call_git_command(
                "git",
                "add",
                log_relative_path,
                cwd=bitswan_dir,
            )
            if not log_add_result:
                raise Exception("Error adding log file to git")

        commit_message = f"Add build context {build_context_hash}"
        if image_tag:
            commit_message += f" for image {image_tag}"
        if build_status:
            commit_message += f" ({build_status})"
        await call_git_command(
            "git",
            "commit",
            "-m",
            commit_message,
            cwd=bitswan_dir,
        )

        subprocess.run(["chown", "-R", "1000:1000", "/gitops/gitops"], check=False)

        if has_remote:
            res = await call_git_command("git", "push", cwd=bitswan_dir)
            if not res:
                raise Exception("Error pushing to git")


async def merge_bitswan_yaml(src_path: str, dst_path: str):
    """
    Merge bitswan.yaml files by combining deployments from both files.
    """
    try:
        # Load existing bitswan.yaml if it exists
        existing_yaml = {}
        if os.path.exists(dst_path):
            with open(dst_path, "r") as f:
                existing_yaml = yaml.safe_load(f) or {}

        # Load new bitswan.yaml from worktree
        new_yaml = {}
        if os.path.exists(src_path):
            with open(src_path, "r") as f:
                new_yaml = yaml.safe_load(f) or {}

        # Merge deployments
        merged_yaml = existing_yaml.copy()
        if "deployments" not in merged_yaml:
            merged_yaml["deployments"] = {}

        if "deployments" in new_yaml:
            merged_yaml["deployments"].update(new_yaml["deployments"])

        # Write merged yaml
        with open(dst_path, "w") as f:
            yaml.dump(merged_yaml, f, default_flow_style=False, sort_keys=False)

    except Exception:
        shutil.copy2(src_path, dst_path)


async def merge_worktree(worktree_path: str, repo: str):
    for item in os.listdir(worktree_path):
        if item == ".git":
            continue

        src_path = os.path.join(worktree_path, item)
        dst_path = os.path.join(repo, item)

        if item == "bitswan.yaml":
            await merge_bitswan_yaml(src_path, dst_path)
            continue

        if os.path.exists(dst_path):
            if os.path.isdir(dst_path):
                shutil.rmtree(dst_path)
            else:
                os.remove(dst_path)

        if os.path.isdir(src_path):
            shutil.copytree(src_path, dst_path)
        else:
            shutil.copy2(src_path, dst_path)


async def copy_worktree(branch_name: str = None):
    """
    Create a temp worktree for the target branch, copy files to main repo, then clean up.
    This works regardless of whether the current directory is already a worktree.

    Uses async lock with optimized hold time - file copying is done outside the lock.
    """
    bs_home = os.environ.get("BITSWAN_GITOPS_DIR", "/mnt/repo/pipeline")
    repo = os.path.join(bs_home, "gitops")

    temp_dir = tempfile.mkdtemp(prefix=f"gitops_worktree_{branch_name}_")
    worktree_path = os.path.join(temp_dir, "worktree")

    try:
        # Use async lock for git operations
        async with GitLockContext(timeout=15.0):
            if not await call_git_command(
                "git", "fetch", "origin", "--prune", "--tags", cwd=repo
            ):
                raise HTTPException(
                    status_code=500, detail="Failed to fetch from origin"
                )
            if not await call_git_command(
                "git", "rev-parse", f"origin/{branch_name}", cwd=repo
            ):
                raise HTTPException(
                    status_code=404,
                    detail=f"Remote branch origin/{branch_name} not found",
                )

            if not await call_git_command(
                "git",
                "worktree",
                "add",
                worktree_path,
                f"origin/{branch_name}",
                cwd=repo,
            ):
                raise HTTPException(
                    status_code=409,
                    detail=f"Failed to create worktree for origin/{branch_name}",
                )
            if not await call_git_command("git", "reset", "--hard", "HEAD", cwd=repo):
                raise HTTPException(
                    status_code=409, detail="Failed to reset working tree"
                )

        # File merge operations don't need the git lock
        await merge_worktree(worktree_path, repo)

        # Re-acquire lock for staging and committing
        async with GitLockContext(timeout=10.0):
            if not await call_git_command("git", "add", "-A", cwd=repo):
                raise HTTPException(status_code=409, detail="Failed to stage files")

            msg = f"Switch to content from origin/{branch_name} using worktree"
            await call_git_command("git", "commit", "-m", msg, cwd=repo)

            has_remote = await call_git_command(
                "git", "remote", "show", "origin", cwd=repo
            )
            if has_remote:
                if not await call_git_command(
                    "git", "push", "-u", "origin", "HEAD", cwd=repo
                ):
                    print(
                        f"Warning: Push failed for branch {branch_name}, continuing anyway"
                    )

            # Remove worktree while holding lock to prevent conflicts
            try:
                if os.path.exists(worktree_path):
                    await call_git_command(
                        "git", "worktree", "remove", worktree_path, cwd=repo
                    )
            except Exception as e:
                print(f"Failed to remove worktree: {e}")

    finally:
        try:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
        except Exception as e:
            print(f"Failed to remove temp directory: {e}")
