import asyncio
import datetime

from fastapi import APIRouter, Depends, Form

from app.dependencies import get_jupyter_service
from app.models import JupyterServerHeartbeatRequest
from app.services.jupyter_service import JupyterService

router = APIRouter(prefix="/jupyter", tags=["jupyter"])


jupyter_servers = {}


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
):
    for server in heartbeat.servers:
        jupyter_servers[server.session_id] = {
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

            jupyter_server_containers = jupyter_service.get_jupyter_servers()

            for container in jupyter_server_containers:
                session_id = container.labels["bitswan.session_id"]

                not_active = session_id not in jupyter_servers
                stale_heartbeat = session_id in jupyter_servers and jupyter_servers[
                    session_id
                ]["last_heartbeat"] < datetime.now() - datetime.timedelta(minutes=10)

                if not_active or stale_heartbeat:
                    jupyter_service.remove_jupyter_server(container.name)
                    del jupyter_servers[session_id]

    except asyncio.CancelledError:
        pass
