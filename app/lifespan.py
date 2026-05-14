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
    process_service,
    publish_processes,
    setup_mqtt_subscriptions,
)
from .routes.worktrees import (
    get_cached_worktrees,
    refresh_worktrees,
)

logger = logging.getLogger(__name__)


async def _broadcast_processes() -> None:
    """Push the current `processes` snapshot over the SSE feed.

    Consumed by the workspace dashboard so it never has to walk the
    filesystem itself. Driven from both `WorkspaceChangeHandler` (main repo)
    and `WorktreeChangeHandler` (per-worktree).
    """
    try:
        await event_broadcaster.broadcast(
            "processes", process_service.get_all_processes()
        )
    except Exception as e:
        logger.warning("Failed to broadcast processes: %s", e)


async def _broadcast_worktrees() -> None:
    """Push the current `worktrees` snapshot over the SSE feed.

    Carries the same payload as `GET /worktrees/`. Driven by
    `WorktreeChangeHandler` so the dashboard never has to poll.
    """
    try:
        worktrees = await refresh_worktrees()
        await event_broadcaster.broadcast("worktrees", worktrees)
    except Exception as e:
        logger.warning("Failed to broadcast worktrees: %s", e)


async def _broadcast_automations() -> None:
    """Push the current `automations` snapshot over the SSE feed.

    Driven by the filesystem watchers (when `bitswan.yaml` or any
    `automation.toml` changes) and by the Docker event watcher. Reads from
    `AutomationService.get_automations()` which now consults the scope-keyed
    cache and overlays live Docker state on read.
    """
    try:
        automations = await get_automation_service().get_automations()
        data = [
            a.model_dump(mode="json") if hasattr(a, "model_dump") else a
            for a in automations
        ]
        await event_broadcaster.broadcast("automations", data)
    except Exception as e:
        logger.warning("Failed to broadcast automations: %s", e)


class WorkspaceChangeHandler(FileSystemEventHandler):
    """Handle file system changes in the workspace directory.

    Two parallel pipelines:
      - process refresh (`process.toml` create/delete/move, plus directory
        events that add/remove BPs).
      - automation refresh (`automation.toml` / `bitswan.yaml` changes).

    Both share the same coarse "anything happened" trigger for create/
    delete/move events, since those almost always coincide with BP or
    automation creation. `on_modified` is filtered by basename so editing
    a python file inside an automation doesn't fire either refresh.
    """

    def __init__(self, client, event_loop):
        super().__init__()
        self.client = client
        self.event_loop = event_loop
        self.update_scheduled = False
        self.update_lock = threading.Lock()
        self.automations_scheduled = False
        self.automations_lock = threading.Lock()

    def schedule_update(self):
        """Schedule a process-cache refresh + broadcast (debounced 500ms)."""
        with self.update_lock:
            if self.update_scheduled:
                return
            self.update_scheduled = True

        async def delayed_update():
            await asyncio.sleep(0.5)  # debounce 500ms for bursts
            try:
                # Refresh the main-repo BP cache before downstream publishers
                # read from it.
                process_service.refresh(None)
                await publish_processes(self.client)
                await _broadcast_processes()
            except Exception as e:
                logger.warning("Error publishing processes: %s", e)
            finally:
                with self.update_lock:
                    self.update_scheduled = False

        asyncio.run_coroutine_threadsafe(delayed_update(), self.event_loop)

    def schedule_automations_update(self):
        """Schedule an automation-cache refresh + broadcast (debounced 500ms).

        Runs independently of the process pipeline so concurrent events don't
        clobber each other. `refresh_all` is cheap (single bitswan.yaml read
        + per-scope filesystem scan).
        """
        with self.automations_lock:
            if self.automations_scheduled:
                return
            self.automations_scheduled = True

        async def delayed_update():
            await asyncio.sleep(0.5)
            try:
                await get_automation_service().refresh_all()
                await _broadcast_automations()
            except Exception as e:
                logger.warning("Error refreshing automations: %s", e)
            finally:
                with self.automations_lock:
                    self.automations_scheduled = False

        asyncio.run_coroutine_threadsafe(delayed_update(), self.event_loop)

    def on_created(self, event):
        self.schedule_update()
        self.schedule_automations_update()

    def on_deleted(self, event):
        self.schedule_update()
        self.schedule_automations_update()

    def on_moved(self, event):
        self.schedule_update()
        self.schedule_automations_update()

    def on_modified(self, event):
        src = getattr(event, "src_path", "") or ""
        if src.endswith("process.toml"):
            self.schedule_update()
        if src.endswith("automation.toml") or src.endswith("bitswan.yaml"):
            self.schedule_automations_update()


class WorktreeChangeHandler(FileSystemEventHandler):
    """Watch worktree directories for file changes and broadcast via SSE.

    Narrowly filtered so editing code inside an automation doesn't fire any
    refresh — only events that meaningfully change the state we publish:
      - worktrees-list ping: events at the worktrees root (worktree added /
        removed), or events under a worktree's `.git/` (commit detection,
        which flips `synced` / `commit_hash`).
      - per-worktree process refresh: `process.toml` events.
      - per-worktree automation refresh: `automation.toml` events.
      - per-worktree automation refresh-all on `bitswan.yaml` events
        (defensive — bitswan.yaml normally lives at the gitops root, not
        inside a worktree).

    The worktree name is derived from the event path so we only re-scan the
    affected scope.
    """

    # Path-suffix markers that flip git state we publish — commits, index
    # writes, branch tip moves. Anything else under `.git/` (e.g. pack file
    # housekeeping) is ignored.
    _GIT_STATE_SUFFIXES = ("/.git/HEAD", "/.git/index", "/.git/ORIG_HEAD")
    _GIT_REFS_SEGMENT = "/.git/refs/heads/"

    def __init__(self, event_loop, worktrees_root: str):
        super().__init__()
        self.event_loop = event_loop
        self.worktrees_root = os.path.realpath(worktrees_root)
        # Per-worktree debounce timers, one set per refresh pipeline.
        self._process_tasks: dict[str | None, asyncio.Task] = {}
        self._automation_tasks: dict[str | None, asyncio.Task] = {}
        # Single timer for the "ping" worktrees-list broadcast.
        self._wt_ping_task: asyncio.Task | None = None

    def _worktree_from_path(self, path: str) -> str | None:
        """Return the name of the worktree containing `path`, or None when
        the event is at the worktrees root (e.g. a worktree being added /
        removed)."""
        try:
            rel = os.path.relpath(os.path.realpath(path), self.worktrees_root)
        except ValueError:
            return None
        if rel == "." or rel.startswith(".."):
            return None
        first = rel.replace("\\", "/").split("/", 1)[0]
        return first or None

    def _is_git_state_change(self, path: str) -> bool:
        """True for paths whose change can flip `synced` / `commit_hash`."""
        if not path:
            return False
        norm = path.replace("\\", "/")
        if norm.endswith(self._GIT_STATE_SUFFIXES):
            return True
        return self._GIT_REFS_SEGMENT in norm

    def _schedule_worktrees_ping(self):
        """Refresh the cached worktree list and broadcast it over SSE."""
        async def _broadcast():
            await asyncio.sleep(1)
            await _broadcast_worktrees()

        def _run():
            if self._wt_ping_task and not self._wt_ping_task.done():
                self._wt_ping_task.cancel()
            self._wt_ping_task = asyncio.ensure_future(_broadcast())

        self.event_loop.call_soon_threadsafe(_run)

    def _schedule_process_refresh(self, worktree: str | None):
        """Debounced refresh + SSE broadcast for one worktree's BP cache.

        `worktree=None` means the event hit the worktrees root itself — a
        worktree was probably added or removed. Refresh the full set so the
        cache reflects the new shape, then broadcast.
        """
        async def _refresh():
            await asyncio.sleep(0.5)
            try:
                if worktree is None:
                    process_service.refresh_all()
                else:
                    if os.path.isdir(
                        os.path.join(self.worktrees_root, worktree)
                    ):
                        process_service.refresh(worktree)
                    else:
                        process_service.forget_worktree(worktree)
                await _broadcast_processes()
            except Exception as e:
                logger.warning(
                    "Failed to refresh worktree processes (%s): %s",
                    worktree or "<root>",
                    e,
                )

        def _run():
            existing = self._process_tasks.get(worktree)
            if existing and not existing.done():
                existing.cancel()
            self._process_tasks[worktree] = asyncio.ensure_future(_refresh())

        self.event_loop.call_soon_threadsafe(_run)

    def _schedule_automations_refresh(self, worktree: str | None):
        """Debounced refresh + SSE broadcast for one worktree's automation cache."""
        async def _refresh():
            await asyncio.sleep(0.5)
            try:
                svc = get_automation_service()
                if worktree is None:
                    await svc.refresh_all()
                else:
                    if os.path.isdir(
                        os.path.join(self.worktrees_root, worktree)
                    ):
                        await svc.refresh(worktree)
                    else:
                        svc.forget_worktree(worktree)
                await _broadcast_automations()
            except Exception as e:
                logger.warning(
                    "Failed to refresh worktree automations (%s): %s",
                    worktree or "<root>",
                    e,
                )

        def _run():
            existing = self._automation_tasks.get(worktree)
            if existing and not existing.done():
                existing.cancel()
            self._automation_tasks[worktree] = asyncio.ensure_future(_refresh())

        self.event_loop.call_soon_threadsafe(_run)

    def _handle(self, event):
        src = getattr(event, "src_path", "") or ""
        wt = self._worktree_from_path(src) if src else None
        basename = os.path.basename(src)

        # 1. Worktree-list ping: only on root events (add/remove) or git
        #    state changes (commit, index write, ref update) — NOT on every
        #    code edit inside a worktree.
        if wt is None or self._is_git_state_change(src):
            self._schedule_worktrees_ping()

        # 2. Worktree directory itself appearing / disappearing → full
        #    refresh of both pipelines so the new scope is picked up (or
        #    stale scope dropped).
        if wt is None:
            self._schedule_process_refresh(None)
            self._schedule_automations_refresh(None)
            return

        # 3. Targeted refresh by file basename. Skip everything else (code
        #    edits, asset writes, etc.) to keep noise off the SSE feed.
        if basename == "process.toml":
            self._schedule_process_refresh(wt)
        if basename == "automation.toml":
            self._schedule_automations_refresh(wt)
        if basename == "bitswan.yaml":
            # bitswan.yaml normally lives at the gitops root, not under a
            # worktree, but treat any in-worktree occurrence as a global
            # automation-state change.
            self._schedule_automations_refresh(None)

    def on_created(self, event):
        self._handle(event)

    def on_deleted(self, event):
        self._handle(event)

    def on_modified(self, event):
        self._handle(event)

    def on_moved(self, event):
        self._handle(event)


async def _broadcast_automations_after_delay():
    """Debounced broadcast of automation state after Docker events settle.

    Cache is unchanged here — `get_automations()` overlays live Docker state
    on the static cache on every call, so a Docker event just needs to
    trigger a re-broadcast.
    """
    await asyncio.sleep(0.5)
    await _broadcast_automations()


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

        # Warm the ProcessService cache before publishing anything — both
        # `publish_processes` (MQTT) and `_broadcast_processes` (SSE) read
        # from it.
        process_service.refresh_all()

        # Warm the AutomationService scope-keyed cache. Cheap (filesystem
        # scan + bitswan.yaml read) and means the first GET /automations/
        # or SSE consumer doesn't pay for an inline `refresh_all()`.
        try:
            await get_automation_service().refresh_all()
        except Exception as e:
            logger.warning("Initial automations cache warm failed: %s", e)

        # Warm the worktree-list cache so the first SSE consumer doesn't
        # pay the git-cost of an initial scan.
        try:
            await refresh_worktrees()
        except Exception as e:
            logger.warning("Initial worktrees cache warm failed: %s", e)

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
            wt_handler = WorktreeChangeHandler(
                asyncio.get_event_loop(), worktrees_dir
            )
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
