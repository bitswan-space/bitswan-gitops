import asyncio
from configparser import ConfigParser
from datetime import datetime, timezone
import hashlib
import os
from typing import Any
import shlex
import subprocess
import shutil
import tempfile

from filelock import FileLock
import humanize
import yaml
import requests
from fastapi import HTTPException


async def wait_coroutine(*args, **kwargs) -> int:
    coro = await asyncio.create_subprocess_exec(*args, **kwargs)
    result = await coro.wait()
    return result


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


async def call_git_command(*command, **kwargs) -> bool:
    cwd = kwargs.get("cwd")
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

        nsenter_command = [
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
        result = await wait_coroutine(*nsenter_command)
        return result == 0

    # Fallback to local git command
    result = await wait_coroutine(*command, **kwargs)
    return result == 0


def read_pipeline_conf(source_dir: str) -> ConfigParser | None:
    conf_file_path = os.path.join(source_dir, "pipelines.conf")
    if os.path.exists(conf_file_path):
        config = ConfigParser()
        config.read(conf_file_path)
        return config
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


async def update_git(
    bitswan_home: str, bitswan_home_host: str, deployment_id: str, action: str
):
    host_path = os.environ.get("HOST_PATH")

    if host_path:
        bitswan_dir = bitswan_home_host
    else:
        bitswan_dir = bitswan_home

    lock_file = os.path.join(bitswan_home, "bitswan_git.lock")

    bitswan_yaml_path = os.path.join(bitswan_dir, "bitswan.yaml")

    lock = FileLock(lock_file, timeout=30)

    with lock:
        has_remote = await call_git_command(
            "git", "remote", "show", "origin", cwd=bitswan_dir
        )

        if has_remote:
            res = await call_git_command("git", "pull", cwd=bitswan_dir)
            if not res:
                raise Exception("Error pulling from git")

        await call_git_command("git", "add", bitswan_yaml_path, cwd=bitswan_dir)

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

        if has_remote:
            res = await call_git_command("git", "push", cwd=bitswan_dir)
            if not res:
                raise Exception("Error pushing to git")


async def docker_compose_up(
    bitswan_dir: str, docker_compose: str, container_name: str | None = None
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

    up_result = await setup_asyncio_process(docker_compose_cmd)

    return {
        "up_result": up_result,
    }


async def save_image(build_context_path: str, build_context_hash: str, image_tag: str):
    """
    Save and commit the build context (extracted zip contents) to git.
    Uses the build_context_hash as the directory name for deduplication.
    """

    bs_home = os.environ.get("BITSWAN_GITOPS_DIR", "/mnt/repo/pipeline")
    bitswan_dir = os.path.join(bs_home, "gitops")

    images_base_dir = os.path.join(bitswan_dir, "images")
    image_dir = os.path.join(images_base_dir, build_context_hash)

    try:
        if not os.path.exists(images_base_dir):
            subprocess.run(["mkdir", "-p", images_base_dir], check=True)
            subprocess.run(["chown", "-R", "1000:1000", images_base_dir], check=False)

        # Create the specific image directory
        os.makedirs(image_dir, exist_ok=True)

    except Exception as e:
        print(f"Error creating directory {image_dir}: {e}")

    # Copy the build context to the gitops directory
    for item in os.listdir(build_context_path):
        src = os.path.join(build_context_path, item)
        dst = os.path.join(image_dir, item)
        if os.path.isdir(src):
            if os.path.exists(dst):
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
        else:
            shutil.copy2(src, dst)

    lock_file = os.path.join(bitswan_dir, "bitswan_git.lock")
    lock = FileLock(lock_file, timeout=30)

    with lock:
        has_remote = await call_git_command(
            "git", "remote", "show", "origin", cwd=bitswan_dir
        )

        if has_remote:
            res = await call_git_command("git", "pull", cwd=bitswan_dir)
            if not res:
                raise Exception("Error pulling from git")

        add_result = await call_git_command(
            "git", "add", os.path.join("images", build_context_hash), cwd=bitswan_dir
        )
        if not add_result:
            raise Exception("Error adding files to git")

        commit_message = f"Add build context {build_context_hash}"
        if image_tag:
            commit_message += f" for image {image_tag}"
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
    """
    bs_home = os.environ.get("BITSWAN_GITOPS_DIR", "/mnt/repo/pipeline")
    repo = os.path.join(bs_home, "gitops")

    lock = FileLock(os.path.join(repo, "bitswan_git.lock"), timeout=30)
    with lock:
        if not await call_git_command(
            "git", "fetch", "origin", "--prune", "--tags", cwd=repo
        ):
            raise HTTPException(status_code=500, detail="Failed to fetch from origin")
        if not await call_git_command(
            "git", "rev-parse", f"origin/{branch_name}", cwd=repo
        ):
            raise HTTPException(
                status_code=404, detail=f"Remote branch origin/{branch_name} not found"
            )

        temp_dir = tempfile.mkdtemp(prefix=f"gitops_worktree_{branch_name}_")
        worktree_path = os.path.join(temp_dir, "worktree")

        try:
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

            await merge_worktree(worktree_path, repo)

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
                    raise HTTPException(status_code=500, detail="Push failed")

        finally:
            try:
                if os.path.exists(worktree_path):
                    await call_git_command(
                        "git", "worktree", "remove", worktree_path, cwd=repo
                    )
            except Exception as e:
                print(f"Failed to remove worktree: {e}")

            try:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
            except Exception as e:
                print(f"Failed to remove temp directory: {e}")

        return {
            "status": "ok",
            "message": f"Switched to content from origin/{branch_name} using worktree",
        }
