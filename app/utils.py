import asyncio
from configparser import ConfigParser
from datetime import datetime, timezone
import hashlib
import os
from typing import Any
import shlex

from filelock import FileLock
import humanize
import yaml
import requests


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
        host_command = f'PATH={host_path} su - {host_user} -c "cd {cwd} && PATH={host_path} HOME={host_home} {formatted_command}"'

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


def generate_url(workspace_id, deployment_id, gitops_domain, full=False):
    url = "{}-{}.{}".format(workspace_id, deployment_id, gitops_domain)
    return f"https://{url}" if full else url


def add_route_to_caddy(deployment_id: str, port: str) -> bool:
    caddy_url = os.environ.get("CADDY_URL", "http://caddy:2019")
    upstreams = requests.get(f"{caddy_url}/reverse_proxy/upstreams")
    gitops_domain = os.environ.get("BITSWAN_GITOPS_DOMAIN", "gitops.bitswan.space")
    workspace_id = os.environ.get("BITSWAN_GITOPS_ID", "workspace-local")
    endpoint = generate_url(workspace_id, deployment_id, gitops_domain, False)

    if upstreams.status_code != 200:
        return False

    upstreams = upstreams.json()
    for upstream in upstreams:
        name = upstream.get("address").split(":")[0]
        # deployment_id is already in the upstreams
        if name == endpoint:
            return True

    body = [
        {
            "@id": get_caddy_id(deployment_id, workspace_id),
            "match": [{"host": [endpoint]}],
            "handle": [
                {
                    "handler": "subroute",
                    "routes": [
                        {
                            "handle": [
                                {
                                    "handler": "reverse_proxy",
                                    "upstreams": [
                                        {"dial": "{}__{}:{}".format(workspace_id, deployment_id, port)}
                                    ],
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


def get_caddy_id(deployment_id, workspace_name):
    return "{}.{}".format(deployment_id, workspace_name)


def remove_route_from_caddy(deployment_id: str, workspace_name: str):
    caddy_url = os.environ.get("CADDY_URL", "http://caddy:2019")
    routes_url = "{}/id/{}".format(
        caddy_url, get_caddy_id(deployment_id, workspace_name)
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
