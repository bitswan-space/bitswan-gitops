# this is a python file that does the
# equivalent of uvicorn app.main:app --port 8079 --host 0.0.0.0
import uvicorn
import app.main
import os


def main():
    debug = os.environ.get("DEBUG", "false").lower() == "true"
    log_level = "debug" if debug else "info"
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
