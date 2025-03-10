from fastapi import Depends, FastAPI

from app.routes.create_deployment import router as create_deployment_router
from app.routes.deploy import router as deploy_router
from app.routes.list_pres import lifespan
from app.routes.automations import router as automations_router
from app.dependencies import verify_token

app = FastAPI(lifespan=lifespan, dependencies=[Depends(verify_token)])

app.include_router(create_deployment_router)
app.include_router(deploy_router)
app.include_router(automations_router)