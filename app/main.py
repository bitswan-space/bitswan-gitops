from .models import ContainerProperties
from datetime import datetime
import os
import docker
import docker.models.containers
import yaml
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse
from .utils import docker_compose_up, git_pull, process_zip_file, read_bitswan_yaml

app = FastAPI()


@app.post("/create/{deployment_id}")
async def upload_zip(deployment_id: str, file: UploadFile = File(...)):
    if file.filename.endswith(".zip"):
        result = await process_zip_file(file, deployment_id)
        return JSONResponse(content=result)
    else:
        return JSONResponse(
            content={"error": "File must be a ZIP archive"}, status_code=400
        )


@app.get("/deploy")
async def deploy():
    bitswan_dir = os.environ.get("BS_BITSWAN_DIR", "/mnt/repo/pipeline")
    bitswan_yaml_path = os.path.join(bitswan_dir, "bitswan.yaml")

    await git_pull(bitswan_dir)

    try:
        bs_yaml = read_bitswan_yaml(bitswan_yaml_path)
    except Exception as e:
        return JSONResponse(content={"error": e}, status_code=500)

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

        entry["environment"] = {"DEPLOYMENT_ID": deployment_id}
        entry["container_name"] = deployment_id
        entry["restart"] = "always"
        entry["labels"] = {
            "gitops.deployment_id": deployment_id,
        }

        if ("env_dir" in bs_yaml) and os.path.exists(
            env_file := os.path.join(bs_yaml["env_dir"], deployment_id)
        ):
            entry["env_file"] = env_file
        if os.path.exists(env_file := os.path.join(bs_yaml["env_dir"], "default")):
            entry["env_file"] = env_file

        if "network_mode" in conf:
            entry["network_mode"] = conf["network_mode"]
        elif "networks" in conf:
            entry["networks"] = conf["networks"].copy()
        elif "default-networks" in bs_yaml:
            entry["networks"] = bs_yaml["default-networks"].copy()

        passthroughs = ["volumes", "ports", "devices", "container_name"]
        entry.update({p: conf[p] for p in passthroughs if p in conf})

        source = conf.get("source", deployment_id)
        deployment_dir = os.path.join(bitswan_dir, source)

        entry["image"] = "bitswan/pipeline-runtime-environment:latest"
        if "volumes" not in entry:
            entry["volumes"] = []
        entry["volumes"].append(f"{deployment_dir}:/conf")

        if conf.get("enabled", True):
            dc["services"][deployment_id] = entry

    dc_yaml = yaml.dump(dc)
    print(dc_yaml)

    # deployment_result = await docker_compose_up(bitswan_dir, dc_yaml, deployments)

    # if any([result["return_code"]] for result in deployment_result.values()):
    #     return JSONResponse(
    #         content={"error": "Error deploying services"}, status_code=500
    #     )

    return JSONResponse(content={"message": dc_yaml})


@app.get("/pres")
async def list_pres() -> list[ContainerProperties]:
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
