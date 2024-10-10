import asyncio
import os
from typing import Any

import yaml


async def git_pull(bitswan_dir: str) -> bool:
    pull_proc = await asyncio.create_subprocess_exec("git", "pull", cwd=bitswan_dir)
    await pull_proc.wait()

    if pull_proc.returncode:
        return False

    return True


async def configure_git() -> None:
    bitswan_dir = os.environ.get("BS_BITSWAN_DIR", "/mnt/repo/pipeline")
    abspath = os.path.abspath(bitswan_dir)

    await asyncio.create_subprocess_exec(
        "git", "config", "--global", "--add", "safe.directory", abspath
    )
    await asyncio.create_subprocess_exec(
        "git", "config", "user.name", "pipeline-ops", cwd=bitswan_dir
    )
    await asyncio.create_subprocess_exec(
        "git", "config", "user.email", "info@bitswan.space", cwd=bitswan_dir
    )
    await asyncio.create_subprocess_exec(
        "git", "config", "pull.rebase", "false", cwd=bitswan_dir
    )


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
