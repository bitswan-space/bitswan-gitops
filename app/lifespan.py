import asyncio
from contextlib import asynccontextmanager
import os
import functools
import threading

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


from .mqtt import mqtt_resource
from .mqtt_publish_automations import publish_automations
from .mqtt_processes import (
    publish_processes,
    setup_mqtt_subscriptions,
)


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    observer = None

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

    try:
        yield
    finally:
        if observer:
            observer.stop()
            # Run blocking join in executor to avoid blocking event loop
            await asyncio.to_thread(observer.join)
