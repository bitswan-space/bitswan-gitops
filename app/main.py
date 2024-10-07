from fastapi import FastAPI

from .routes.create_deployment import router as create_deployment_router
from .routes.deploy import router as deploy_router
from .routes.list_pres import lifespan

app = FastAPI(lifespan=lifespan)

app.include_router(create_deployment_router)
app.include_router(deploy_router)
