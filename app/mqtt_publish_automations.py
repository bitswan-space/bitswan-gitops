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
    workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace-local")
    
    active = await retrieve_active_automations()
    inactive = await retrieve_inactive_automations()

    automations = inactive.topology.copy()
    automations.update(active.topology)

    # Include workspace metadata even when there are no automations
    metadata = {
        "workspace_name": workspace_name,
        "timestamp": datetime.now().isoformat(),
        "automation_count": {
            "total": len(automations),
            "active": len(active.topology),
            "inactive": len(inactive.topology)
        }
    }

    topology = Topology(topology=automations, display_style="list", metadata=metadata)
    
    # Log publishing information
    total_automations = len(automations)
    active_count = len(active.topology)
    inactive_count = len(inactive.topology)
    
    print(f"Publishing topology for workspace '{workspace_name}': {total_automations} total automations ({active_count} active, {inactive_count} inactive)")

    try:
        result = client.publish(
            topic,
            payload=encode_pydantic_model(topology),
            qos=1,
            retain=True,
        )
        
        if result.rc == mqtt_client.MQTT_ERR_SUCCESS:
            print(f"Successfully published to MQTT topic '{topic}'")
        else:
            print(f"Failed to publish to MQTT topic '{topic}', return code: {result.rc}")
            
    except Exception as e:
        print(f"ERROR: Exception occurred while publishing to MQTT: {e}")

    return topology


async def diagnose_mqtt_publishing() -> dict:
    """
    Diagnostic function to help troubleshoot MQTT publishing issues.
    Returns information about the current state and configuration.
    """
    workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace-local")
    bs_home = os.environ.get("BITSWAN_BITSWAN_DIR", "/mnt/repo/pipeline")
    mqtt_broker = os.environ.get("MQTT_BROKER")
    mqtt_topic = os.environ.get("MQTT_TOPIC", "/topology")
    
    # Check bitswan.yaml status
    bs_yaml = read_bitswan_yaml(bs_home)
    bitswan_yaml_exists = bs_yaml is not None
    deployments_count = len(bs_yaml.get("deployments", {})) if bs_yaml else 0
    
    # Check automations
    active = await retrieve_active_automations()
    inactive = await retrieve_inactive_automations()
    
    # Check MQTT connection
    mqtt_configured = mqtt_broker is not None
    mqtt_connection_status = "unknown"
    if mqtt_configured:
        try:
            result = await mqtt_resource.connect()
            mqtt_connection_status = "connected" if result else "failed"
        except Exception as e:
            mqtt_connection_status = f"error: {e}"
    
    return {
        "workspace_name": workspace_name,
        "bitswan_home": bs_home,
        "bitswan_yaml_exists": bitswan_yaml_exists,
        "deployments_in_yaml": deployments_count,
        "active_automations": len(active.topology),
        "inactive_automations": len(inactive.topology),
        "mqtt_broker": mqtt_broker,
        "mqtt_topic": mqtt_topic,
        "mqtt_configured": mqtt_configured,
        "mqtt_connection_status": mqtt_connection_status,
        "environment_vars": {
            "BITSWAN_WORKSPACE_NAME": os.environ.get("BITSWAN_WORKSPACE_NAME"),
            "BITSWAN_BITSWAN_DIR": os.environ.get("BITSWAN_BITSWAN_DIR"),
            "MQTT_BROKER": os.environ.get("MQTT_BROKER"),
            "MQTT_PORT": os.environ.get("MQTT_PORT"),
            "MQTT_USERNAME": os.environ.get("MQTT_USERNAME") is not None,
            "MQTT_PASSWORD": os.environ.get("MQTT_PASSWORD") is not None,
            "MQTT_TOPIC": os.environ.get("MQTT_TOPIC"),
        }
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = AsyncIOScheduler(timezone="UTC")
    result = await mqtt_resource.connect()

    if result:
        print("MQTT connection successful, starting automation publishing scheduler")
        scheduler.add_job(
            functools.partial(publish_automations, mqtt_resource.get_client()),
            trigger="interval",
            seconds=10,
        )
        scheduler.start()
    else:
        print("MQTT connection failed - automation publishing will not be available")
        print("Check MQTT_BROKER, MQTT_USERNAME, MQTT_PASSWORD environment variables")
    
    yield
    
    # Clean shutdown
    if scheduler.running:
        scheduler.shutdown()
