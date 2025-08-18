import docker
import docker.models.containers
import os
from datetime import datetime
from paho.mqtt import client as mqtt_client

from .models import (
    ContainerProperties,
    Topology,
    Pipeline,
    encode_pydantic_model,
)
from .utils import calculate_uptime, read_bitswan_yaml, generate_workspace_url


async def retrieve_active_automations() -> Topology:
    client = docker.from_env()
    workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace-local")
    gitops_domain = os.environ.get("BITSWAN_GITOPS_DOMAIN", None)
    bs_home = os.environ.get("BITSWAN_GITOPS_DIR", "/mnt/repo/pipeline")
    gitops_dir = os.path.join(bs_home, "gitops")

    # Read bitswan.yaml for relative path information
    bs_yaml = read_bitswan_yaml(gitops_dir)

    containers: list[docker.models.containers.Container] = client.containers.list(
        filters={
            "label": [
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
                    "automation_url": get_automation_url(
                        c, workspace_name, gitops_domain
                    ),
                    "relative_path": get_relative_path(
                        c.labels["gitops.deployment_id"], bs_yaml
                    ),
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


def get_automation_url(
    container, workspace_name: str, gitops_domain: str | None
) -> str | None:
    """Generate automation URL if the container is exposed"""
    if not gitops_domain:
        return None

    # Check if container is intended to be exposed
    label = container.attrs["Config"]["Labels"].get("gitops.intended_exposed", "false")
    if label != "true":
        return None

    deployment_id = container.labels["gitops.deployment_id"]
    return generate_workspace_url(workspace_name, deployment_id, gitops_domain, True)


def get_relative_path(deployment_id: str, bs_yaml: dict | None) -> str | None:
    """Get relative path from bitswan.yaml"""
    if not bs_yaml or "deployments" not in bs_yaml:
        return None

    deployment_config = bs_yaml["deployments"].get(deployment_id, {})
    return deployment_config.get("relative_path")


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
            automation_url=None,
            relative_path=bs_yaml["deployments"][deployment_id].get("relative_path"),
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
