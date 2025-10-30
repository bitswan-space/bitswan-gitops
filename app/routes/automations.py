from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from app.services.automation_service import AutomationService
from app.dependencies import get_automation_service

router = APIRouter(prefix="/automations", tags=["automations"])


@router.get("/")
async def get_automations(
    automation_service: AutomationService = Depends(get_automation_service),
):
    return automation_service.get_automations()


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
    file: UploadFile = File(None),
    relative_path: str = Form(None),
    automation_service: AutomationService = Depends(get_automation_service),
):
    return await automation_service.deploy_automation(deployment_id, file, relative_path)


@router.post("/{deployment_id}/promote")
async def promote_automation(
    deployment_id: str,
    checksum: str = Form(...),
    automation_service: AutomationService = Depends(get_automation_service),
):
    return await automation_service.promote_automation(deployment_id, checksum)


@router.post("/{deployment_id}/start")
async def start_automation(
    deployment_id: str,
    automation_service: AutomationService = Depends(get_automation_service),
):
    return automation_service.start_automation(deployment_id)


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
    return automation_service.restart_automation(deployment_id)


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
    return automation_service.get_automation_logs(deployment_id, lines)


@router.post("/{deployment_id}")
async def create_automation(
    deployment_id: str,
    file: UploadFile = File(...),
    relative_path: str = Form(None),
    automation_service: AutomationService = Depends(get_automation_service),
):
    if file.filename.endswith(".zip"):
        result = await automation_service.create_automation(
            deployment_id, file, relative_path
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
