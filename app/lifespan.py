import asyncio
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from app.dependencies import get_jupyter_service
from app.routes.jupyter import jupyter_server_cleanup

from .mqtt import mqtt_resource
from .mqtt_publish_automations import publish_automations
from .mqtt_processes import (
    publish_processes,
    setup_mqtt_subscriptions,
)


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
        client = mqtt_resource.get_client()

        # Set up MQTT subscriptions for process operations
        await setup_mqtt_subscriptions(client)

        async def publish_mqtt_data():
            await publish_processes(client)
            await publish_automations(client)

        # Schedule periodic updates
        scheduler.add_job(
            publish_mqtt_data,
            trigger="interval",
            seconds=10,
            name="publish_mqtt_data",
        )

        scheduler.start()

    try:
        yield
    finally:
        stop.set()
        jupyter_server_cleanup_task.cancel()
        await jupyter_server_cleanup_task
