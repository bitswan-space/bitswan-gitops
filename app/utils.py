import asyncio
from configparser import ConfigParser
from datetime import datetime, timezone
import os
from typing import Any
import shlex
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


def add_route_to_caddy(deployment_id: str, port: str) -> bool:
    caddy_url = os.environ.get("CADDY_URL", "http://caddy:2019")
    upstreams = requests.get(f"{caddy_url}/reverse_proxy/upstreams")
    gitops_domain = os.environ.get("BITSWAN_GITOPS_DOMAIN", "gitops.bitswan.space")

    if upstreams.status_code != 200:
        return False

    upstreams = upstreams.json()
    for upstream in upstreams:
        name = upstream.get("address").split(":")[0]
        # deployment_id is already in the upstreams
        if name == deployment_id:
            return True

    body = [
        {
            "match": [{"host": ["{}.{}".format(deployment_id, gitops_domain)]}],
            "handle": [
                {
                    "handler": "subroute",
                    "routes": [
                        {
                            "handle": [
                                {
                                    "handler": "reverse_proxy",
                                    "upstreams": [
                                        {"dial": "{}:{}".format(deployment_id, port)}
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
