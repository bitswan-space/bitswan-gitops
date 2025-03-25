# this is a python file that does the
# equivalent of uvicorn app.main:app --port 8079 --host 0.0.0.0
import uvicorn
import app.main
import os


def main():
    debug = os.environ.get("DEBUG", "false").lower() == "true"
    log_level = "debug" if debug else "info"
    uvicorn.run(
        app.main.app,
        host="0.0.0.0",
        port=8079,
        log_level=log_level,
    )
