# this is a python file that does the
# equivalent of uvicorn app.main:app --port 8079 --host 0.0.0.0
import uvicorn
import app.main
import os


def _setup_profiling():
    """Start yappi async-aware profiler and return a dump function, or None if disabled."""
    profiling_enabled = os.environ.get("BITSWAN_PROFILING", "").lower() in (
        "1",
        "true",
        "yes",
    )
    if not profiling_enabled:
        return None

    import yappi
    import datetime
    import signal
    import logging

    logger = logging.getLogger(__name__)

    workspace_dir = os.environ.get("BITSWAN_WORKSPACE_REPO_DIR", "/workspace-repo")
    profiling_dir = os.path.join(workspace_dir, "profiling")
    os.makedirs(profiling_dir, exist_ok=True)

    yappi.set_clock_type("wall")
    yappi.start(builtins=False)
    logger.info("Profiling enabled — output directory: %s", profiling_dir)

    def dump_profile(sig=None, frame=None):
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(profiling_dir, f"profile_{ts}.pstat")
        stats = yappi.get_func_stats()
        stats.save(path, type="pstat")
        logger.info("Profile written to %s", path)

    # Dump on SIGUSR1 (kill -USR1 <pid>) for on-demand snapshots
    signal.signal(signal.SIGUSR1, dump_profile)

    return dump_profile


def main():
    debug = os.environ.get("DEBUG", "false").lower() == "true"
    log_level = "debug" if debug else "info"

    dump_profile = _setup_profiling()

    try:
        if debug:
            # reload=True requires the app as a string import path, not an object.
            # reload_dirs watches the mounted dev source so edits take effect immediately.
            uvicorn.run(
                "app.main:app",
                host="0.0.0.0",
                port=8079,
                log_level=log_level,
                reload=True,
                reload_dirs=["/src"],
            )
        else:
            uvicorn.run(
                app.main.app,
                host="0.0.0.0",
                port=8079,
                log_level=log_level,
            )
    finally:
        if dump_profile is not None:
            dump_profile()
