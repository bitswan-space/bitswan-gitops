import asyncio
from contextlib import asynccontextmanager
import logging
import os
import functools
import threading

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


from .async_docker import get_async_docker_client
from .dependencies import get_automation_service
from .event_broadcaster import event_broadcaster
from .mqtt import mqtt_resource
from .mqtt_publish_automations import publish_automations
from .mqtt_processes import (
    publish_processes,
    setup_mqtt_subscriptions,
)

logger = logging.getLogger(__name__)


class WorkspaceChangeHandler(FileSystemEventHandler):
    """Handle file system changes in the workspace directory."""

    def __init__(self, client, event_loop):
        super().__init__()
        self.client = client
        self.event_loop = event_loop
        self.update_scheduled = False
        self.update_lock = threading.Lock()

    def schedule_update(self):
        """Schedule an update to avoid multiple rapid updates."""
        with self.update_lock:
            if self.update_scheduled:
                return
            self.update_scheduled = True

        # Use asyncio.sleep to debounce updates
        async def delayed_update():
            await asyncio.sleep(0.5)  # Wait 500ms for multiple events
            try:
                await publish_processes(self.client)
            except Exception as e:
                print(f"Error publishing processes: {e}")
            finally:
                with self.update_lock:
                    self.update_scheduled = False

        asyncio.run_coroutine_threadsafe(delayed_update(), self.event_loop)

    def on_created(self, event):
        self.schedule_update()

    def on_deleted(self, event):
        self.schedule_update()

    def on_moved(self, event):
        self.schedule_update()

    def on_modified(self, event):
        if event.src_path.endswith("process.toml"):
            self.schedule_update()


async def _broadcast_automations_after_delay():
    """Debounced broadcast of automation state after Docker events settle."""
    await asyncio.sleep(0.5)
    try:
        automations = await get_automation_service().get_automations()
        data = [a.model_dump(mode="json") if hasattr(a, "model_dump") else a for a in automations]
        await event_broadcaster.broadcast("automations", data)
    except Exception as e:
        logger.warning("Failed to broadcast automations: %s", e)


async def _docker_event_watcher():
    """Watch Docker container events and broadcast automation state changes."""
    docker_client = get_async_docker_client()
    workspace = os.environ.get("BITSWAN_WORKSPACE_NAME", "")
    debounce_task: asyncio.Task | None = None

    filters: dict = {"type": ["container"]}
    if workspace:
        filters["label"] = [f"gitops.workspace={workspace}"]

    while True:
        try:
            async for event in docker_client.watch_events(filters=filters):
                action = event.get("Action", "")
                if action in ("start", "stop", "die", "destroy", "create"):
                    if debounce_task and not debounce_task.done():
                        debounce_task.cancel()
                    debounce_task = asyncio.create_task(
                        _broadcast_automations_after_delay()
                    )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("Docker event watcher error: %s, reconnecting in 5s", e)
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    observer = None
    watcher_task: asyncio.Task | None = None

    scheduler = AsyncIOScheduler(timezone="UTC")
    result = await mqtt_resource.connect()

    if result:
        client = mqtt_resource.get_client()

        # Set up MQTT subscriptions for process operations
        await setup_mqtt_subscriptions(client)

        # Publish processes and automations
        await publish_processes(client)
        await publish_automations(client)

        # Schedule periodic updates
        scheduler.add_job(
            functools.partial(publish_automations, client),
            trigger="interval",
            seconds=10,
            name="publish_automations",
        )

        # Set up file system watcher for workspace directory
        workspace_dir = os.environ.get("BITSWAN_WORKSPACE_REPO_DIR", "/workspace-repo")
        if os.path.exists(workspace_dir):
            event_handler = WorkspaceChangeHandler(client, asyncio.get_event_loop())
            observer = Observer()
            observer.schedule(event_handler, workspace_dir, recursive=True)
            observer.start()
            print(f"Started watching workspace directory: {workspace_dir}")
        else:
            print(f"Workspace directory does not exist: {workspace_dir}")

        scheduler.start()

    # Warm the history cache in the background so first requests are fast
    asyncio.create_task(get_automation_service().warm_history_cache())

    # Start Docker event watcher for SSE push updates
    watcher_task = asyncio.create_task(_docker_event_watcher())

    try:
        yield
    finally:
        # Stop Docker event watcher
        if watcher_task:
            watcher_task.cancel()
            try:
                await watcher_task
            except asyncio.CancelledError:
                pass

        if observer:
            observer.stop()
            # Run blocking join in executor to avoid blocking event loop
            await asyncio.to_thread(observer.join)
