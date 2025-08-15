import asyncio
import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Form

from app.dependencies import get_jupyter_service
from app.models import JupyterServerHeartbeatRequest
from app.services.jupyter_service import JupyterService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/jupyter", tags=["jupyter"])


active_jupyter_servers: dict[str, dict] = {}


@router.post("/start")
async def start_jupyter_server(
    automation_name: str = Form(...),
    pre_image: str = Form(...),
    session_id: str = Form(...),
    jupyter_service: JupyterService = Depends(get_jupyter_service),
):
    return jupyter_service.start_jupyter_server(automation_name, pre_image, session_id)


@router.post("/heartbeat")
async def heartbeat(
    heartbeat: JupyterServerHeartbeatRequest,
    jupyter_service: JupyterService = Depends(get_jupyter_service),
):
    for server in heartbeat.servers:

        jupyter_server_containers = jupyter_service.get_jupyter_server_containers()
        container_session_ids = {
            container.labels["bitswan.session_id"]
            for container in jupyter_server_containers
        }

        if server.session_id not in container_session_ids:
            logger.info(f"New jupyter server: {server.session_id}")
            jupyter_service.start_jupyter_server(
                server.automation_name,
                server.pre_image,
                server.session_id,
                server_token=server.token,
            )

        active_jupyter_servers[server.session_id] = {
            "automation_name": server.automation_name,
            "session_id": server.session_id,
            "last_heartbeat": datetime.now(),
        }


async def jupyter_server_cleanup(
    stop: asyncio.Event,
    jupyter_service: JupyterService,
):
    try:
        while not stop.is_set():
            await asyncio.sleep(60)
            logger.info("Checking for jupyter server cleanup")

            jupyter_server_containers = jupyter_service.get_jupyter_server_containers(
                all=True
            )
            container_session_ids = {
                container.labels["bitswan.session_id"]
                for container in jupyter_server_containers
            }

            current_time = datetime.now()
            stale_threshold = current_time - timedelta(minutes=2)

            for container in jupyter_server_containers:
                session_id = container.labels["bitswan.session_id"]
                server_info = active_jupyter_servers.get(session_id)

                if not server_info or server_info["last_heartbeat"] < stale_threshold:
                    logger.info(
                        f"Removing inactive/stale jupyter server: {container.name}"
                    )
                    jupyter_service.teardown_jupyter_server_container(container.name)
                    if server_info:
                        del active_jupyter_servers[session_id]

            # Clean up orphaned active servers
            orphaned = set(active_jupyter_servers) - container_session_ids
            for session_id in orphaned:
                logger.info(
                    f"Removing orphaned jupyter server from active list: {session_id}"
                )
                del active_jupyter_servers[session_id]

    except asyncio.CancelledError:
        pass

    except Exception as e:
        logger.error("Error in jupyter server cleanup: ", e)
