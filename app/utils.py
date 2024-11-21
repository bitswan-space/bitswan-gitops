import asyncio
from configparser import ConfigParser
import os
from typing import Any
import shlex

import yaml


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


async def call_git_command(*command, **kwargs) -> bool:
    host_dir = os.environ.get("BS_HOST_DIR", "/mnt/repo/pipeline")
    host_path = os.environ.get("HOST_PATH")
    host_home = os.environ.get("HOST_HOME")
    host_user = os.environ.get("HOST_USER")

    # If all host environment variables are set, use nsenter to run git command on host
    if host_dir and host_path and host_home and host_user:
        host_command = 'PATH={} su - {} -c "cd {} && PATH={} HOME={} {}"'.format(
            host_path,
            host_user,
            host_dir,
            host_path,
            host_home,
            " ".join(shlex.quote(arg) for arg in command),
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


def read_pipeline_conf(source_dir: str) -> dict[str, Any]:
    conf_file_path = os.path.join(source_dir, "pipelines.conf")
    if os.path.exists(conf_file_path):
        config = ConfigParser()
        config.read(conf_file_path)
        return config
    return {}
