import asyncio
import json as _json
import logging
import os
import tempfile

from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    UploadFile,
    Query,
    Request,
    Header,
)
from fastapi.responses import JSONResponse, StreamingResponse

from app.deploy_manager import DeployStatus, DeployStep, deploy_manager
from app.event_broadcaster import event_broadcaster
from app.services.automation_service import AutomationService
from app.dependencies import get_automation_service

logger = logging.getLogger(__name__)

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


async def _run_deploy_with_progress(
    task_id: str,
    deployment_id: str,
    automation_service: AutomationService,
    deploy_kwargs: dict,
):
    """Background coroutine that runs deploy_automation with progress broadcasting."""

    async def progress_callback(step: str, message: str):
        # Never set COMPLETED here — only _run_deploy_with_progress decides success/failure
        deploy_step = DeployStep(step)
        await deploy_manager.update_task(
            task_id,
            status=DeployStatus.IN_PROGRESS,
            step=deploy_step,
            message=message,
        )
        task = deploy_manager.get_task(task_id)
        if task:
            await event_broadcaster.broadcast("deploy_progress", task.to_dict())

    async def _broadcast_task():
        task = deploy_manager.get_task(task_id)
        if task:
            await event_broadcaster.broadcast("deploy_progress", task.to_dict())

    try:
        await deploy_manager.update_task(
            task_id, status=DeployStatus.IN_PROGRESS, message="Starting deployment..."
        )
        await _broadcast_task()

        await automation_service.deploy_automation(
            **deploy_kwargs, progress_callback=progress_callback
        )

        # deploy_automation returned without exception → success
        await deploy_manager.update_task(
            task_id,
            status=DeployStatus.COMPLETED,
            step=DeployStep.DONE,
            message="Deployment completed successfully",
        )
        await _broadcast_task()
    except Exception as exc:
        logger.exception("Deploy failed for %s (task %s)", deployment_id, task_id)
        error_detail = str(exc)
        if hasattr(exc, "detail"):
            error_detail = exc.detail
        await deploy_manager.update_task(
            task_id,
            status=DeployStatus.FAILED,
            error=error_detail,
            message="Deployment failed",
        )
        await _broadcast_task()


@router.get("/deploy-status/{task_id}")
async def get_deploy_status(task_id: str):
    """Poll fallback for SSE drops — returns current deploy task state."""
    task = deploy_manager.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Deploy task not found")
    return task.to_dict()


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
    automation_id: str | None = Form(None),  # Unique automation ID for Keycloak
    auth: str | None = Form(None),  # "true" or "false" - enable Keycloak auth
    allowed_domains: str | None = Form(
        None
    ),  # comma-separated list of CORS allowed domains
    expose_to: str | None = Form(None),  # JSON list: ["/Example Org/admin"]
    services: str | None = Form(None),  # JSON: {"kafka": {"enabled": true}, ...}
    replicas: str | None = Form(None),  # replicas as string from form
    automation_service: AutomationService = Depends(get_automation_service),
):
    # Guard: reject if already deploying
    if deploy_manager.is_deploying(deployment_id):
        raise HTTPException(
            status_code=409,
            detail=f"Deployment {deployment_id} is already in progress",
        )

    # Validate stage if provided
    if stage is not None and stage not in ["dev", "staging", "production", "live-dev"]:
        raise HTTPException(
            status_code=400,
            detail="Stage must be one of: dev, staging, production, live-dev",
        )
    # Convert form values to proper types
    expose_bool = expose.lower() == "true" if expose else None
    port_int = int(port) if port else None
    secret_groups_list = (
        [g.strip() for g in secret_groups.split(",") if g.strip()]
        if secret_groups
        else None
    )
    auth_bool = auth.lower() == "true" if auth else None
    allowed_domains_list = (
        [d.strip() for d in allowed_domains.split(",") if d.strip()]
        if allowed_domains
        else None
    )
    replicas_int = int(replicas) if replicas else None

    expose_to_list = None
    if expose_to:
        try:
            expose_to_list = _json.loads(expose_to)
        except _json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid expose_to JSON")

    services_dict = None
    if services:
        try:
            services_dict = _json.loads(services)
        except _json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid services JSON")

    # Create tracked deploy task
    task = await deploy_manager.create_task(deployment_id)
    if task is None:
        raise HTTPException(
            status_code=409,
            detail=f"Deployment {deployment_id} is already in progress",
        )

    deploy_kwargs = dict(
        deployment_id=deployment_id,
        checksum=checksum,
        stage=stage,
        relative_path=relative_path,
        image=image,
        expose=expose_bool,
        expose_to=expose_to_list,
        port=port_int,
        mount_path=mount_path,
        secret_groups=secret_groups_list,
        automation_id=automation_id,
        auth=auth_bool,
        allowed_domains=allowed_domains_list,
        services=services_dict,
        replicas=replicas_int,
    )

    # Spawn background task — returns 202 immediately
    asyncio.create_task(
        _run_deploy_with_progress(
            task.task_id, deployment_id, automation_service, deploy_kwargs
        )
    )

    return JSONResponse(
        status_code=202,
        content={
            "task_id": task.task_id,
            "deployment_id": deployment_id,
            "status": "pending",
        },
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


@router.post("/{deployment_id}/scale")
async def scale_automation(
    deployment_id: str,
    replicas: str = Form(...),
    automation_service: AutomationService = Depends(get_automation_service),
):
    try:
        replicas_int = int(replicas)
    except ValueError:
        raise HTTPException(status_code=400, detail="replicas must be an integer")
    if replicas_int < 1:
        raise HTTPException(status_code=400, detail="replicas must be at least 1")
    return await automation_service.scale_automation(deployment_id, replicas_int)


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


@router.get("/{deployment_id}/logs/stream")
async def stream_automation_logs(
    deployment_id: str,
    lines: int = Query(200, ge=1, le=10000),
    since: int = Query(0, ge=0),
    automation_service: AutomationService = Depends(get_automation_service),
):
    return StreamingResponse(
        automation_service.stream_automation_logs(
            deployment_id, lines=lines, since=since
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


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
        result = await automation_service.upload_asset_from_path(
            temp_path, checksum=checksum
        )
        return JSONResponse(content=result)
    except HTTPException as exc:
        logger.error("upload_asset_stream failed [%s]: %s", exc.status_code, exc.detail)
        raise
    except Exception as exc:
        logger.exception("upload_asset_stream unexpected error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
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
