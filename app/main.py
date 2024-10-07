from datetime import datetime
import docker
import docker.models.containers
from fastapi import FastAPI

from .routes.create_deployment import router as create_deployment_router
from .routes.deploy import router as deploy_router
from .models import ContainerProperties

app = FastAPI()

app.include_router(create_deployment_router)
app.include_router(deploy_router)


def list_containers() -> list[ContainerProperties]:
    client = docker.from_env()
    info = client.info()

    containers: list[docker.models.containers.Container] = client.containers.list(
        filters={
            "label": [
                "space.bitswan.pipeline.protocol.-version",
                "gitops.deployment_id",
            ]
        }
    )

    attrs = list(
        map(
            lambda c: ContainerProperties(
                c.id,
                info["name"],  # FIXME: i hate docker sdk
                datetime.fromtimestamp(c.attrs["Created"]),
                c.name.replace("/", ""),
                c.attrs["State"],
                c.status,
                c.labels["gitops.deployment_id"],
            ),
            containers,
        )
    )

    return attrs


# TODO: schedule
# FIXME: also find inactive ones through bitswan.yaml
@app.get("/pres")
async def list_pres() -> list[ContainerProperties]:
    return list_containers()
