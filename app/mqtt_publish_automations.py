import functools
import docker
import docker.models.containers
import os
from fastapi import FastAPI
from datetime import datetime
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from paho.mqtt import client as mqtt_client

from .models import (
    ContainerProperties,
    Topology,
    Pipeline,
    encode_pydantic_model,
)
from .utils import calculate_uptime, read_bitswan_yaml
from .mqtt import mqtt_resource


async def retrieve_active_automations() -> Topology:
    client = docker.from_env()
    workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace-local")

    containers: list[docker.models.containers.Container] = client.containers.list(
        filters={
            "label": [
                "space.bitswan.pipeline.protocol-version",
                "gitops.deployment_id",
                f"gitops.workspace={workspace_name}",
            ]
        }
    )

    parsed_containers = list(
        map(
            lambda c: {
                "wires": [],
                "properties": {
                    "container_id": c.id,
                    "endpoint_name": workspace_name,  # FIXME: i hate docker sdk
                    "created_at": datetime.strptime(
                        c.attrs["Created"][:26] + "Z", "%Y-%m-%dT%H:%M:%S.%fZ"
                    ),  # how tf does this work
                    "name": c.name.replace("/", ""),
                    "state": c.status,
                    "status": calculate_uptime(c.attrs["State"]["StartedAt"]),
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


async def retrieve_inactive_automations() -> Topology:
    bs_home = os.environ.get("BITSWAN_BITSWAN_DIR", "/mnt/repo/pipeline")
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


async def publish_automations(client: mqtt_client.Client) -> Topology:
    topic = os.environ.get("MQTT_TOPIC", "/topology")
    active = await retrieve_active_automations()
    inactive = await retrieve_inactive_automations()

    automations = inactive.topology.copy()
    automations.update(active.topology)

    topology = Topology(topology=automations, display_style="list")

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
    result = await mqtt_resource.connect()

    if result:
        scheduler.add_job(
            functools.partial(publish_automations, mqtt_resource.get_client()),
            trigger="interval",
            seconds=10,
        )
        scheduler.start()
    yield
