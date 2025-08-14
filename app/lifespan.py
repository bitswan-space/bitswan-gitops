import asyncio
import functools
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from app.dependencies import get_jupyter_service
from app.routes.jupyter import jupyter_server_cleanup

from .mqtt import mqtt_resource
from .mqtt_publish_automations import publish_automations


@asynccontextmanager
async def lifespan(app: FastAPI):
    stop = asyncio.Event()

    jupyter_service = get_jupyter_service()

    jupyter_server_cleanup_task = asyncio.create_task(
        jupyter_server_cleanup(stop, jupyter_service)
    )

    scheduler = AsyncIOScheduler(timezone="UTC")
    result = await mqtt_resource.connect()

    if result:
        scheduler.add_job(
            functools.partial(publish_automations, mqtt_resource.get_client()),
            trigger="interval",
            seconds=10,
        )
        scheduler.start()

    try:
        yield
    finally:
        stop.set()
        jupyter_server_cleanup_task.cancel()
        await jupyter_server_cleanup_task
