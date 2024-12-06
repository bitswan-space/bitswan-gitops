# this is a python file that does the
# equivalent of uvicorn app.main:app --port 8079 --host 0.0.0.0
import uvicorn
import app.main


def main():
    uvicorn.run(app.main.app, host="0.0.0.0", port=8079)
