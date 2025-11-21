from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from app.services.image_service import ImageService
from app.dependencies import get_image_service

router = APIRouter(prefix="/images", tags=["images"])


@router.get("/")
async def get_images(
    image_service: ImageService = Depends(get_image_service),
):
    return image_service.get_images()


@router.get("/{image_tag}/logs")
async def get_image_logs(
    image_tag: str,
    lines: int = 100,
    image_service: ImageService = Depends(get_image_service),
):
    return image_service.get_image_logs(image_tag, lines)


@router.post("/{image_tag}")
async def create_image(
    image_tag: str,
    file: UploadFile = File(...),
    checksum: str = Form(...),
    image_service: ImageService = Depends(get_image_service),
):
    if file.filename.endswith(".zip"):
        result = await image_service.create_image(image_tag, file, checksum=checksum)
        return JSONResponse(content=result)
    else:
        raise HTTPException(status_code=400, detail="File must be a ZIP archive")


@router.delete("/{image_tag}")
async def delete_image(
    image_tag: str,
    image_service: ImageService = Depends(get_image_service),
):
    return await image_service.delete_image(image_tag)
