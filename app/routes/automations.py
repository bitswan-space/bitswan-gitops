from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, Query, Request, Header
from fastapi.responses import JSONResponse
from app.services.automation_service import AutomationService
from app.dependencies import get_automation_service
import tempfile
import os

router = APIRouter(prefix="/automations", tags=["automations"])


@router.get("/")
async def get_automations(
    automation_service: AutomationService = Depends(get_automation_service),
):
    # Now fully async using aiohttp Docker client
    return await automation_service.get_automations()


@router.post("/deploy")
async def deploy_automations(
    automation_service: AutomationService = Depends(get_automation_service),
):
    return await automation_service.deploy_automations()


@router.post("/pull-and-deploy/{branch_name}")
async def pull_and_deploy(
    branch_name: str,
    automation_service: AutomationService = Depends(get_automation_service),
):
    return await automation_service.pull_and_deploy(branch_name)


@router.post("/{deployment_id}/deploy")
async def deploy_automation(
    deployment_id: str,
    checksum: str | None = Form(None),
    stage: str | None = Form(None),
    relative_path: str | None = Form(None),
    # Automation config values (sent by extension for live-dev)
    image: str | None = Form(None),
    expose: str | None = Form(None),  # "true" or "false" as string from form
    port: str | None = Form(None),  # port as string from form
    mount_path: str | None = Form(None),
    secret_groups: str | None = Form(None),  # comma-separated list of secret groups
    automation_service: AutomationService = Depends(get_automation_service),
):
    # Validate stage if provided
    if stage is not None and stage not in ["dev", "staging", "production", "live-dev"]:
        raise HTTPException(
            status_code=400,
            detail="Stage must be one of: dev, staging, production, live-dev",
        )
    # Convert form values to proper types
    expose_bool = expose.lower() == "true" if expose else None
    port_int = int(port) if port else None
    secret_groups_list = [g.strip() for g in secret_groups.split(",") if g.strip()] if secret_groups else None

    return await automation_service.deploy_automation(
        deployment_id,
        checksum=checksum,
        stage=stage,
        relative_path=relative_path,
        image=image,
        expose=expose_bool,
        port=port_int,
        mount_path=mount_path,
        secret_groups=secret_groups_list,
    )


@router.post("/{deployment_id}/start")
async def start_automation(
    deployment_id: str,
    automation_service: AutomationService = Depends(get_automation_service),
):
    # Now fully async using aiohttp Docker client
    return await automation_service.start_automation(deployment_id)


@router.post("/{deployment_id}/stop")
async def stop_automation(
    deployment_id: str,
    automation_service: AutomationService = Depends(get_automation_service),
):
    return await automation_service.stop_automation(deployment_id)


@router.post("/{deployment_id}/restart")
async def restart_automation(
    deployment_id: str,
    automation_service: AutomationService = Depends(get_automation_service),
):
    # Now fully async using aiohttp Docker client
    return await automation_service.restart_automation(deployment_id)


@router.post("/{deployment_id}/activate")
async def activate_automation(
    deployment_id: str,
    automation_service: AutomationService = Depends(get_automation_service),
):
    return await automation_service.activate_automation(deployment_id)


@router.post("/{deployment_id}/deactivate")
async def deactivate_automation(
    deployment_id: str,
    automation_service: AutomationService = Depends(get_automation_service),
):
    return await automation_service.deactivate_automation(deployment_id)


@router.get("/{deployment_id}/logs")
async def get_automation_logs(
    deployment_id: str,
    lines: int = 100,
    automation_service: AutomationService = Depends(get_automation_service),
):
    # Now fully async using aiohttp Docker client
    return await automation_service.get_automation_logs(deployment_id, lines)


@router.post("/{deployment_id}")
async def create_automation(
    deployment_id: str,
    file: UploadFile = File(...),
    relative_path: str = Form(None),
    checksum: str = Form(...),
    automation_service: AutomationService = Depends(get_automation_service),
):
    if file.filename.endswith(".zip"):
        result = await automation_service.create_automation(
            deployment_id, file, relative_path, checksum=checksum
        )
        return JSONResponse(content=result)
    else:
        raise HTTPException(status_code=400, detail="File must be a ZIP archive")


@router.delete("/{deployment_id}")
async def delete_automation(
    deployment_id: str,
    automation_service: AutomationService = Depends(get_automation_service),
):
    return await automation_service.delete_automation(deployment_id)


@router.post("/assets/upload")
async def upload_asset(
    file: UploadFile = File(...),
    checksum: str = Form(...),
    automation_service: AutomationService = Depends(get_automation_service),
):
    if file.filename.endswith(".zip"):
        result = await automation_service.upload_asset(file, checksum=checksum)
        return JSONResponse(content=result)
    else:
        raise HTTPException(status_code=400, detail="File must be a ZIP archive")


@router.post("/assets/upload-stream")
async def upload_asset_stream(
    request: Request,
    checksum: str = Header(..., alias="X-Checksum"),
    automation_service: AutomationService = Depends(get_automation_service),
):
    """
    Streaming upload endpoint for large zip files.
    Receives raw zip data in the request body with checksum in X-Checksum header.
    This endpoint supports chunked transfer encoding.
    """
    # Create a temporary file to store the streamed data
    with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as temp_file:
        temp_path = temp_file.name
        # Stream the request body to the temp file
        async for chunk in request.stream():
            temp_file.write(chunk)

    try:
        # Create a file-like object for the service
        result = await automation_service.upload_asset_from_path(temp_path, checksum=checksum)
        return JSONResponse(content=result)
    finally:
        # Clean up temp file
        if os.path.exists(temp_path):
            os.unlink(temp_path)


@router.get("/assets")
async def list_assets(
    automation_service: AutomationService = Depends(get_automation_service),
):
    return automation_service.list_assets()


@router.get("/{deployment_id}/history")
async def get_automation_history(
    deployment_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    automation_service: AutomationService = Depends(get_automation_service),
):
    return await automation_service.get_automation_history(
        deployment_id, page=page, page_size=page_size
    )
