import yaml
import os
import asyncio
from typing import Any
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from ..utils import read_bitswan_yaml, call_git_command, read_pipeline_conf

router = APIRouter()


@router.get("/deploy")
async def deploy():
    bitswan_dir = os.environ.get("BS_BITSWAN_DIR", "/mnt/repo/pipeline")
    host_dir = os.environ.get("BS_HOST_DIR", "/mnt/repo/pipeline")
    pipeline_ops_id = os.environ.get("BS_PIPELINEOPS_ID", "pipeline-ops")
    os.environ["COMPOSE_PROJECT_NAME"] = pipeline_ops_id

    await call_git_command("git", "pull", cwd=bitswan_dir)

    bs_yaml = read_bitswan_yaml(bitswan_dir)

    if not bs_yaml:
        return JSONResponse(
            content={"error": "Error reading bitswan.yaml"}, status_code=500
        )

    dc = {
        "version": "3",
        "services": {},
        "networks": {
            network: {"external": True}
            for network in bs_yaml.get("default-networks", {})
        },
    }
    deployments = bs_yaml.get("deployments", {})
    for deployment_id, conf in deployments.items():
        conf = conf or {}
        entry = {}

        source = conf.get("source") or conf.get("checksum") or deployment_id
        source_dir = os.path.join(bitswan_dir, source)

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

        network_mode = pipeline_conf.get(
            "docker.compose", "network_mode", fallback=conf.get("network_mode")
        ) or conf.get("network_mode")

        if network_mode:
            entry["network_mode"] = network_mode
        elif "networks" in conf:
            entry["networks"] = conf["networks"].copy()
        elif "default-networks" in bs_yaml:
            entry["networks"] = bs_yaml["default-networks"].copy()

        passthroughs = ["volumes", "ports", "devices", "container_name"]
        entry.update({p: conf[p] for p in passthroughs if p in conf})

        deployment_dir = os.path.join(host_dir, source)

        entry["image"] = (
            pipeline_conf.get("deployment", "pre", fallback=entry.get("image"))
            or entry["image"]
        )

        if "volumes" not in entry:
            entry["volumes"] = []
        entry["volumes"].append(f"{deployment_dir}:/opt/pipelines")

        if conf.get("enabled", True):
            dc["services"][deployment_id] = entry

    dc_yaml = yaml.dump(dc)

    deployment_result = await docker_compose_up(bitswan_dir, dc_yaml, deployments)

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
