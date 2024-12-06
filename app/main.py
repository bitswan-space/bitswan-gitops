from fastapi import Depends, FastAPI

from .routes.create_deployment import router as create_deployment_router
from .routes.deploy import router as deploy_router
from .routes.list_pres import lifespan
from .dependencies import verify_token

app = FastAPI(lifespan=lifespan, dependencies=[Depends(verify_token)])

app.include_router(create_deployment_router)
app.include_router(deploy_router)
