from fastapi import Depends, FastAPI

from app.mqtt_publish_automations import lifespan
from app.routes.automations import router as automations_router
from app.dependencies import verify_token

app = FastAPI(lifespan=lifespan, dependencies=[Depends(verify_token)])

app.include_router(automations_router)