from dataclasses import asdict
import functools
import docker
import docker.models.containers
import os
import json
from fastapi import FastAPI
from datetime import datetime
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from paho.mqtt import client as mqtt_client

from ..models import ContainerProperties, encode_pydantic_models
from ..utils import read_bitswan_yaml, configure_git
from ..mqtt import mqtt_resource


async def retrieve_active_pres() -> list[ContainerProperties]:
    client = docker.from_env()
    info = client.info()

    containers: list[docker.models.containers.Container] = client.containers.list(
        filters={
            "label": [
                "space.bitswan.pipeline.protocol-version",
                "gitops.deployment_id",
            ]
        }
    )

    parsed_containers = list(
        map(
            lambda c: {
                "container_id": c.id,
                "endpoint_name": info["Name"],  # FIXME: i hate docker sdk
                "created": datetime.strptime(
                    c.attrs["Created"][:26] + "Z", "%Y-%m-%dT%H:%M:%S.%fZ"
                ),  # how tf does this work
                "name": c.name.replace("/", ""),
                "state": c.attrs["State"]["Status"],
                "status": c.status,
                "deployment_id": c.labels["gitops.deployment_id"],
            },
            containers,
        )
    )

    attrs = [ContainerProperties(**c) for c in parsed_containers]

    return attrs


async def retrieve_inactive_pres() -> list[ContainerProperties]:
    bs_home = os.environ.get("BS_BITSWAN_DIR", "/mnt/repo/pipeline")
    bs_yaml = read_bitswan_yaml(bs_home)

    if not bs_yaml:
        return []
    else:
        return [
            ContainerProperties(
                container_id=None,
                endpoint_name=None,
                created=None,
                name=deployment_id,
                state=None,
                status=None,
                deployment_id=deployment_id,
            )
            for deployment_id in bs_yaml["deployments"]
            if not bs_yaml["deployments"][deployment_id].get("active", False)
        ]


async def publish_pres(client: mqtt_client.Client) -> list[ContainerProperties]:
    active = await retrieve_active_pres()
    inactive = await retrieve_inactive_pres()

    pres = active + inactive

    client.publish(
        "bitswan/topology",
        payload=encode_pydantic_models(pres),
        qos=1,
        retain=True,
    )

    return active + inactive


@asynccontextmanager
async def lifespan(app: FastAPI):
    await configure_git()
    scheduler = AsyncIOScheduler(timezone="UTC")
    await mqtt_resource.connect()

    scheduler.add_job(
        functools.partial(publish_pres, mqtt_resource.get_client()),
        trigger="interval",
        seconds=10,
    )
    scheduler.start()
    yield
