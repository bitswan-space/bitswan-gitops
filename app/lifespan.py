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
from .deploy_manager import deploy_manager
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


class WorktreeChangeHandler(FileSystemEventHandler):
    """Watch worktree directories for file changes and broadcast via SSE."""

    def __init__(self, event_loop):
        super().__init__()
        self.event_loop = event_loop
        self._debounce_task: asyncio.Task | None = None

    def _schedule_broadcast(self):
        async def _broadcast():
            await asyncio.sleep(1)  # debounce 1s
            try:
                await event_broadcaster.broadcast("worktrees", {})
            except Exception as e:
                logger.warning("Failed to broadcast worktree change: %s", e)

        def _run():
            if self._debounce_task and not self._debounce_task.done():
                self._debounce_task.cancel()
            self._debounce_task = asyncio.ensure_future(_broadcast())

        self.event_loop.call_soon_threadsafe(_run)

    def on_created(self, event):
        self._schedule_broadcast()

    def on_deleted(self, event):
        self._schedule_broadcast()

    def on_modified(self, event):
        self._schedule_broadcast()

    def on_moved(self, event):
        self._schedule_broadcast()


async def _broadcast_automations_after_delay():
    """Debounced broadcast of automation state after Docker events settle."""
    await asyncio.sleep(0.5)
    try:
        automations = await get_automation_service().get_automations()
        data = [
            a.model_dump(mode="json") if hasattr(a, "model_dump") else a
            for a in automations
        ]
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


def _start_profiling():
    """Start yappi async-aware profiler if BITSWAN_PROFILING is set.

    Returns a dump callable (also wired to SIGUSR1) or None when disabled.
    Re-evaluated on every lifespan startup so hot-reload picks up env changes.
    """
    enabled = os.environ.get("BITSWAN_PROFILING", "").lower() in ("1", "true", "yes")
    if not enabled:
        return None

    import yappi
    import datetime
    import signal

    workspace_dir = os.environ.get("BITSWAN_WORKSPACE_REPO_DIR", "/workspace-repo")
    profiling_dir = os.path.join(workspace_dir, "profiling")
    os.makedirs(profiling_dir, exist_ok=True)

    # Stop any leftover session from a previous reload before starting fresh.
    yappi.stop()
    prior_stats = yappi.get_func_stats()
    if not prior_stats.empty():
        ts = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        prior_path = os.path.join(profiling_dir, f"profile_{ts}.pstat")
        prior_stats.save(prior_path, type="pstat")
        logger.info("Saved previous profiling session to %s", prior_path)
    yappi.clear_stats()
    yappi.set_clock_type("wall")
    yappi.start(builtins=False)
    logger.info("=" * 60)
    logger.info("  PROFILING ACTIVE (yappi, wall-clock, asyncio-aware)")
    logger.info("  Output: %s", profiling_dir)
    logger.info("  Dump now: kill -USR1 %d", os.getpid())
    logger.info("=" * 60)

    def dump_profile(sig=None, frame=None):
        ts = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        path = os.path.join(profiling_dir, f"profile_{ts}.pstat")
        yappi.get_func_stats().save(path, type="pstat")
        logger.info("Profile written to %s", path)

    # SIGUSR1 → on-demand snapshot without stopping the server.
    signal.signal(signal.SIGUSR1, dump_profile)

    return dump_profile


@asynccontextmanager
async def lifespan(app: FastAPI):
    observer = None
    worktree_observer = None
    watcher_task: asyncio.Task | None = None
    dump_profile = _start_profiling()

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

        # Watch worktree directories for file changes → SSE push
        worktrees_dir = os.path.join(workspace_dir, "worktrees")
        if os.path.exists(worktrees_dir):
            wt_handler = WorktreeChangeHandler(asyncio.get_event_loop())
            worktree_observer = Observer()
            worktree_observer.schedule(wt_handler, worktrees_dir, recursive=True)
            worktree_observer.start()
            print(f"Started watching worktrees directory: {worktrees_dir}")

        # Clean up completed/failed deploy tasks every 10 minutes
        scheduler.add_job(
            deploy_manager.cleanup_old_tasks,
            trigger="interval",
            minutes=10,
            name="cleanup_deploy_tasks",
        )

        # Daily backup at 2 AM UTC (if configured)
        async def _scheduled_backup():
            from app.services.backup_service import (
                get_backup_config,
                get_restic_key,
                run_backup,
            )

            config = get_backup_config()
            if not config or not get_restic_key():
                return  # Not configured, skip
            try:
                await run_backup(config)
                print("Scheduled backup completed successfully")
            except Exception as e:
                print(f"Scheduled backup failed: {e}")

        scheduler.add_job(
            _scheduled_backup,
            trigger="cron",
            hour=2,
            minute=0,
            name="daily_backup",
        )

        scheduler.start()

    # Warm the history cache in the background so first requests are fast
    _cache_task = asyncio.create_task(get_automation_service().warm_history_cache())
    _cache_task.add_done_callback(
        lambda t: (
            logger.warning("warm_history_cache failed: %s", t.exception())
            if not t.cancelled() and t.exception()
            else None
        )
    )

    # Start Docker event watcher for SSE push updates
    watcher_task = asyncio.create_task(_docker_event_watcher())

    docker_client = get_async_docker_client()

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

        # Explicitly close the Docker client session so aiohttp doesn't report
        # "Unclosed client session" warnings during interpreter shutdown
        await docker_client.close()

        if observer:
            observer.stop()
            # Run blocking join in executor to avoid blocking event loop
            await asyncio.to_thread(observer.join)
        if worktree_observer:
            worktree_observer.stop()
            await asyncio.to_thread(worktree_observer.join)

        if dump_profile is not None:
            dump_profile()
