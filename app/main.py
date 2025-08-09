import os

from fastapi import Depends, FastAPI

from app.dependencies import verify_token
from app.mqtt_publish_automations import lifespan
from app.routes.automations import router as automations_router
from app.routes.images import router as images_router
from app.routes.jupyter import router as jupyter_router

debug = os.environ.get("DEBUG", "false").lower() == "true"

app = FastAPI(lifespan=lifespan, dependencies=[Depends(verify_token)], debug=debug)

app.include_router(automations_router)
app.include_router(images_router)
app.include_router(jupyter_router)
