import functools
import docker
import docker.models.containers
import os
import humanize
from fastapi import FastAPI
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from paho.mqtt import client as mqtt_client

from ..models import ContainerProperties, Topology, Pipeline, encode_pydantic_model
from ..utils import read_bitswan_yaml
from ..mqtt import mqtt_resource


def calculate_uptime(created_at: str) -> str:
    created_at = datetime.fromisoformat(created_at)
    uptime = datetime.now(timezone.utc) - created_at
    return humanize.naturaldelta(uptime)


async def retrieve_active_pres() -> Topology:
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
                "wires": [],
                "properties": {
                    "container_id": c.id,
                    "endpoint_name": info["Name"],  # FIXME: i hate docker sdk
                    "created_at": datetime.strptime(
                        c.attrs["Created"][:26] + "Z", "%Y-%m-%dT%H:%M:%S.%fZ"
                    ),  # how tf does this work
                    "name": c.name.replace("/", ""),
                    "state": c.status,
                    "status": calculate_uptime(c.attrs["Created"]),
                    "deployment_id": c.labels["gitops.deployment_id"],
                },
                "metrics": [],
            },
            containers,
        )
    )

    topology = {
        "topology": {
            c["properties"]["deployment_id"]: Pipeline(
                wires=c["wires"],
                properties=ContainerProperties(**c["properties"]),
                metrics=c["metrics"],
            )
            for c in parsed_containers
        },
        "display_style": "list",
    }

    return Topology(**topology)


async def retrieve_inactive_pres() -> Topology:
    bs_home = os.environ.get("BS_BITSWAN_DIR", "/mnt/repo/pipeline")
    bs_yaml = read_bitswan_yaml(bs_home)

    if not bs_yaml:
        return Topology(topology={}, display_style="list")

    # Create list of inactive containers
    inactive_containers = [
        ContainerProperties(
            container_id=None,
            endpoint_name=None,
            created_at=None,
            name=deployment_id,
            state=None,
            status=None,
            deployment_id=deployment_id,
        )
        for deployment_id in bs_yaml["deployments"]
        if not bs_yaml["deployments"][deployment_id].get("active", False)
    ]

    # Build topology with inactive containers
    topology = {
        "topology": {
            container.name: Pipeline(
                wires=[],  # Wires are empty for inactive containers
                properties=container,
                metrics=[],  # Metrics can be filled as needed
            )
            for container in inactive_containers
        },
        "display_style": "list",
    }

    # Return Topology instance
    return Topology(**topology)


async def publish_pres(client: mqtt_client.Client) -> Topology:
    topic = os.environ.get("MQTT_TOPIC", "bitswan/topology")
    active = await retrieve_active_pres()
    inactive = await retrieve_inactive_pres()

    pres = inactive.topology.copy()
    pres.update(active.topology)

    topology = Topology(topology=pres, display_style="list")

    client.publish(
        topic,
        payload=encode_pydantic_model(topology),
        qos=1,
        retain=True,
    )

    return topology


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = AsyncIOScheduler(timezone="UTC")
    await mqtt_resource.connect()

    scheduler.add_job(
        functools.partial(publish_pres, mqtt_resource.get_client()),
        trigger="interval",
        seconds=10,
    )
    scheduler.start()
    yield
