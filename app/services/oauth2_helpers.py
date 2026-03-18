"""Shared oauth2-proxy helpers for injecting the proxy into containers."""

import asyncio
import logging

logger = logging.getLogger(__name__)

OAUTH2_PROXY_PATH = "/usr/local/bin/oauth2-proxy"

# Shell command to check if oauth2-proxy is running inside a container.
# Uses /proc/*/comm instead of pgrep/grep which may not exist in minimal images.
OAUTH2_PROXY_CHECK_CMD = [
    "sh",
    "-c",
    'for f in /proc/[0-9]*/comm; do if [ "$(cat $f 2>/dev/null)" = "oauth2-proxy" ]; then exit 0; fi; done; exit 1',
]


async def is_oauth2_proxy_running(docker_client, container_id: str) -> bool:
    """Check if oauth2-proxy is already running inside a container."""
    exec_id = await docker_client.exec_create(container_id, OAUTH2_PROXY_CHECK_CMD)
    await docker_client.exec_start(exec_id)
    exec_info = await docker_client.exec_inspect(exec_id)
    return exec_info.get("ExitCode") == 0


async def copy_oauth2_proxy_to_container(
    container_id: str, container_name: str
) -> bool:
    """Copy the oauth2-proxy binary into a container. Returns True on success."""
    logger.info(f"Copying oauth2-proxy into {container_name}")
    proc = await asyncio.create_subprocess_exec(
        "docker",
        "cp",
        OAUTH2_PROXY_PATH,
        f"{container_id}:/usr/local/bin/oauth2-proxy",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        logger.error(
            f"Failed to copy oauth2-proxy into {container_name}: {stderr.decode()}"
        )
        return False
    return True
