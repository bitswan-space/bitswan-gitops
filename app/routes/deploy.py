import yaml
import os
import asyncio
from typing import Any
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from ..utils import (
    read_bitswan_yaml,
    read_pipeline_conf,
    add_route_to_caddy,
)

router = APIRouter()


@router.post("/deploy")
async def deploy():
    gitops_dir = os.environ.get(
        "BITSWAN_GITOPS_DIR",
        "/gitops",
    )
    host_dir = os.environ.get(
        "BITSWAN_GITOPS_DIR_HOST", "/home/root/.config/bitswan/local-gitops/"
    )
    gitops_id = os.environ.get("BITSWAN_GITOPS_ID", "gitops-local")
    os.environ["COMPOSE_PROJECT_NAME"] = gitops_id

    gitops_config = os.path.join(gitops_dir, "gitops")
    gitops_config_host = os.path.join(host_dir, "gitops")
    secrets_dir = os.path.join(gitops_dir, "secrets")

    bs_yaml = read_bitswan_yaml(gitops_config)

    if not bs_yaml:
        return JSONResponse(
            content={"error": "Error reading bitswan.yaml"}, status_code=500
        )

    dc = {
        "version": "3",
        "services": {},
    }
    external_networks = {"bitswan_network"}
    deployments = bs_yaml.get("deployments", {})
    for deployment_id, conf in deployments.items():
        conf = conf or {}
        entry = {}

        source = conf.get("source") or conf.get("checksum") or deployment_id
        source_dir = os.path.join(gitops_config, source)

        if not os.path.exists(source_dir):
            return JSONResponse(
                content={"error": f"Deployment directory {source_dir} does not exist"},
                status_code=500,
            )
        else:
            pipeline_conf = read_pipeline_conf(source_dir)

        entry["environment"] = {"DEPLOYMENT_ID": deployment_id}
        entry["container_name"] = deployment_id
        entry["restart"] = "always"
        entry["labels"] = {
            "gitops.deployment_id": deployment_id,
        }
        entry["image"] = "bitswan/pipeline-runtime-environment:latest"

        network_mode = None
        secret_groups = []
        if pipeline_conf:
            network_mode = pipeline_conf.get(
                "docker.compose", "network_mode", fallback=conf.get("network_mode")
            )
            secret_groups = pipeline_conf.get("secrets", "groups", fallback="").split(
                " "
            )
        for secret_group in secret_groups:
            # Skip empty secret groups
            if not secret_group:
                continue
            if os.path.exists(secrets_dir):
                secret_env_file = os.path.join(secrets_dir, secret_group)
                if os.path.exists(secret_env_file):
                    if not entry.get("env_file"):
                        entry["env_file"] = []
                    entry["env_file"].append(secret_env_file)

        if not network_mode:
            network_mode = conf.get("network_mode")

        if network_mode:
            entry["network_mode"] = network_mode
        elif "networks" in conf:
            entry["networks"] = conf["networks"].copy()
        elif "default-networks" in bs_yaml:
            entry["networks"] = bs_yaml["default-networks"].copy()
        else:
            entry["networks"] = ["bitswan_network"]
        if entry.get("networks"):
            external_networks.update(set(entry["networks"]))

        passthroughs = ["volumes", "ports", "devices", "container_name"]
        entry.update({p: conf[p] for p in passthroughs if p in conf})

        deployment_dir = os.path.join(gitops_config_host, source)

        if pipeline_conf:
            entry["image"] = (
                pipeline_conf.get("deployment", "pre", fallback=entry.get("image"))
                or entry["image"]
            )
            expose = pipeline_conf.getboolean(
                "deployment", "expose", fallback=conf.get("expose")
            )
            port = pipeline_conf.get(
                "deployment", "port", fallback=conf.get("port", 8080)
            )
            if expose and port:
                result = add_route_to_caddy(deployment_id, port)
                if not result:
                    return JSONResponse(
                        content={"error": "Error adding route to Caddy"},
                        status_code=500,
                    )

        if "volumes" not in entry:
            entry["volumes"] = []
        entry["volumes"].append(f"{deployment_dir}:/opt/pipelines")

        if conf.get("enabled", True):
            dc["services"][deployment_id] = entry

    dc["networks"] = {}
    for network in external_networks:
        dc["networks"][network] = {"external": True}
    dc_yaml = yaml.dump(dc)

    deployment_result = await docker_compose_up(gitops_config, dc_yaml, deployments)

    for result in deployment_result.values():
        if result["return_code"] != 0:
            return JSONResponse(
                content={"error": "Error deploying services"}, status_code=500
            )
    return JSONResponse(
        content={
            "message": "Deployed services successfully",
            "deployments": list(deployments.keys()),
            "result": deployment_result,
        }
    )


async def docker_compose_up(
    bitswan_dir: str, docker_compose: str, deployment_info: dict[str, Any]
) -> None:
    async def setup_asyncio_process(cmd: list[Any]) -> dict[str, Any]:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=bitswan_dir,
        )

        stdout, stderr = await proc.communicate(input=docker_compose.encode())

        res = {
            "cmd": cmd,
            "stdout": stdout.decode("utf-8"),
            "stderr": stderr.decode("utf-8"),
            "return_code": proc.returncode,
        }
        return res

    up_result = await setup_asyncio_process(
        ["docker", "compose", "-f", "/dev/stdin", "up", "-d", "--remove-orphans"]
    )

    return {
        "up_result": up_result,
    }
