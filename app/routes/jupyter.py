from fastapi import APIRouter, Depends, Form

from app.dependencies import get_jupyter_service
from app.services.jupyter_service import JupyterService

router = APIRouter(prefix="/jupyter", tags=["jupyter"])


@router.post("/start")
async def start_jupyter_server(
    automation_name: str = Form(...),
    pre_image: str = Form(...),
    jupyter_service: JupyterService = Depends(get_jupyter_service),
):
    return jupyter_service.start_jupyter_server(automation_name, pre_image)
