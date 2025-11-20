from fastapi import APIRouter, Depends, Form, Path

from app.dependencies import get_jupyter_service
from app.services.jupyter_service import JupyterService

router = APIRouter(prefix="/jupyter", tags=["jupyter"])


@router.get("/kernels")
async def list_kernels(
    jupyter_service: JupyterService = Depends(get_jupyter_service),
):
    """List all Jupyter kernel containers with connection details."""
    return await jupyter_service.list_kernels_with_details()


@router.get("/kernels/{deployment_id}")
async def get_kernel(
    deployment_id: str = Path(...),
    jupyter_service: JupyterService = Depends(get_jupyter_service),
):
    """Get kernel status and connection details for a deployment_id."""
    return await jupyter_service.get_kernel_details(deployment_id)


@router.post("/kernels/{deployment_id}/start")
async def start_kernel(
    deployment_id: str = Path(...),
    relative_path: str = Form(""),
    pipelines_conf_content: str = Form(""),
    jupyter_service: JupyterService = Depends(get_jupyter_service),
):
    """Start a Jupyter kernel for a deployment_id."""
    return await jupyter_service.start_kernel(
        deployment_id=deployment_id,
        relative_path=relative_path,
        pipelines_conf_content=pipelines_conf_content,
    )


@router.post("/kernels/{deployment_id}/stop")
async def stop_kernel(
    deployment_id: str = Path(...),
    jupyter_service: JupyterService = Depends(get_jupyter_service),
):
    """Stop a Jupyter kernel for a deployment_id."""
    return await jupyter_service.stop_kernel(deployment_id=deployment_id)
