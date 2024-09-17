from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse
from .utils import process_zip_file

app = FastAPI()

@app.post("/create/{deployment_id}")
async def upload_zip(deployment_id: str, file: UploadFile = File(...)):
    if file.filename.endswith(".zip"):
        result = await process_zip_file(file, deployment_id)
        return JSONResponse(content=result)
    else:
        return JSONResponse(
            content={"error": "File must be a ZIP archive"}, status_code=400
        )
