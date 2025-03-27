from datetime import datetime
import os
import shutil
from tempfile import NamedTemporaryFile
import zipfile
import docker
import yaml
from app.models import DeployedAutomation
from app.utils import (
    add_route_to_caddy,
    calculate_checksum,
    calculate_uptime,
    docker_compose_up,
    read_bitswan_yaml,
    read_pipeline_conf,
    remove_route_from_caddy,
    update_git,
)
from fastapi import UploadFile, HTTPException


class AutomationService:
    def __init__(self):
        self.bs_home = os.environ.get("BITSWAN_GITOPS_DIR", "/mnt/repo/pipeline")
        self.bs_home_host = os.environ.get(
            "BITSWAN_GITOPS_DIR_HOST", "/home/root/.config/bitswan/local-gitops/"
        )
        self.gitops_id = os.environ.get("BITSWAN_GITOPS_ID", "gitops-local")
        self.docker_client = docker.from_env()
        self.gitops_dir = os.path.join(self.bs_home, "gitops")
        self.gitops_dir_host = os.path.join(self.bs_home_host, "gitops")
        self.secrets_dir = os.path.join(self.bs_home, "secrets")

    def get_workspace_name(self):
        return os.path.basename(self.gitops_dir_host)

    def get_container(self, deployment_id):
        return self.docker_client.containers.list(
            all=True,  # Include stopped containers
            filters={
                "label": [
                    "space.bitswan.pipeline.protocol-version",
                    f"gitops.deployment_id={deployment_id}",
                    f"gitops.workspace={self.get_workspace_name()}",
                ]
            },
        )

    def get_containers(self):
        return self.docker_client.containers.list(
            all=True,  # Include stopped containers
            filters={
                "label": [
                    "space.bitswan.pipeline.protocol-version",
                    "gitops.deployment_id",
                    f"gitops.workspace={self.get_workspace_name()}",
                ]
            },
        )

    def get_automations(self):
        bs_yaml = read_bitswan_yaml(self.gitops_dir)

        if not bs_yaml:
            return []

        pres = {
            deployment_id: DeployedAutomation(
                container_id=None,
                endpoint_name=None,
                created_at=None,
                name=deployment_id,
                state=None,
                status=None,
                deployment_id=deployment_id,
                active=bs_yaml["deployments"][deployment_id].get("active", False),
            )
            for deployment_id in bs_yaml["deployments"]
        }

        info = self.docker_client.info()
        containers: list[docker.models.containers.Container] = self.get_containers()

        # updated pres with active containers
        for container in containers:
            deployment_id = container.labels["gitops.deployment_id"]
            if deployment_id in pres:
                pres[deployment_id] = DeployedAutomation(
                    container_id=container.id,
                    endpoint_name=info["Name"],
                    created_at=datetime.strptime(
                        container.attrs["Created"][:26] + "Z", "%Y-%m-%dT%H:%M:%S.%fZ"
                    ),
                    name=deployment_id,
                    state=container.status,
                    status=calculate_uptime(container.attrs["State"]["StartedAt"]),
                    deployment_id=deployment_id,
                    active=pres[deployment_id].active,
                )

        return list(pres.values())

    async def create_automation(self, deployment_id: str, file: UploadFile):
        with NamedTemporaryFile(delete=False) as temp_file:
            content = await file.read()
            temp_file.write(content)

            checksum = calculate_checksum(temp_file.name)
            output_dir = f"{checksum}"
            old_deploymend_checksum = None

            try:
                bitswan_yaml_path = os.path.join(self.gitops_dir, "bitswan.yaml")

                output_dir = os.path.join(self.gitops_dir, output_dir)

                os.makedirs(output_dir, exist_ok=True)
                temp_file.close()
                with zipfile.ZipFile(temp_file.name, "r") as zip_ref:
                    zip_ref.extractall(output_dir)

                # Update or create bitswan.yaml
                data = read_bitswan_yaml(self.gitops_dir)

                data = data or {"deployments": {}}
                deployments = data["deployments"]  # should never raise KeyError

                deployments[deployment_id] = deployments.get(deployment_id, {})
                old_deploymend_checksum = deployments[deployment_id].get("checksum")
                deployments[deployment_id]["checksum"] = checksum
                deployments[deployment_id]["active"] = True

                data["deployments"] = deployments
                with open(bitswan_yaml_path, "w") as f:
                    yaml.dump(data, f)

                await update_git(
                    self.gitops_dir, self.gitops_dir_host, deployment_id, "create"
                )

                return {
                    "message": "File processed successfully",
                    "output_directory": output_dir,
                    "checksum": checksum,
                }
            except Exception as e:
                shutil.rmtree(output_dir, ignore_errors=True)
                return {"error": f"Error processing file: {str(e)}"}
            finally:
                if old_deploymend_checksum:
                    shutil.rmtree(
                        os.path.join(self.gitops_dir, old_deploymend_checksum),
                        ignore_errors=True,
                    )
                os.unlink(temp_file.name)

    async def delete_automation(self, deployment_id: str):
        bs_yaml = read_bitswan_yaml(self.gitops_dir)
        bs_yaml["deployments"].pop(deployment_id)
        with open(os.path.join(self.gitops_dir, "bitswan.yaml"), "w") as f:
            yaml.dump(bs_yaml, f)

        await update_git(self.gitops_dir, self.gitops_dir_host, deployment_id, "delete")
        result = remove_route_from_caddy(deployment_id, self.get_workspace_name())

        if not result:
            message = f"Deployment {deployment_id} deleted successfully, but failed to remove route from Caddy"
        else:
            message = f"Deployment {deployment_id} deleted successfully"

        self.remove_automation(deployment_id)
        return {"status": "success", "message": message}

    async def deploy_automation(self, deployment_id: str):
        os.environ["COMPOSE_PROJECT_NAME"] = self.gitops_id

        bs_yaml = read_bitswan_yaml(self.gitops_dir)

        if not bs_yaml:
            raise HTTPException(status_code=500, detail="Error reading bitswan.yaml")

        dc_yaml = self.generate_docker_compose(bs_yaml)
        deployments = bs_yaml.get("deployments", {})

        deployment_result = await docker_compose_up(
            self.gitops_dir, dc_yaml, deployment_id
        )

        for result in deployment_result.values():
            if result["return_code"] != 0:
                raise HTTPException(status_code=500, detail="Error deploying services")
        return {
            "message": "Deployed services successfully",
            "deployments": list(deployments[deployment_id].keys()),
            "result": deployment_result,
        }

    async def deploy_automations(self):
        os.environ["COMPOSE_PROJECT_NAME"] = self.gitops_id

        bs_yaml = read_bitswan_yaml(self.gitops_dir)

        if not bs_yaml:
            raise HTTPException(status_code=500, detail="Error reading bitswan.yaml")

        dc_yaml = self.generate_docker_compose(bs_yaml)
        deployments = bs_yaml.get("deployments", {})

        deployment_result = await docker_compose_up(self.gitops_dir, dc_yaml)

        for result in deployment_result.values():
            if result["return_code"] != 0:
                raise HTTPException(status_code=500, detail="Error deploying services")
        return {
            "message": "Deployed services successfully",
            "deployments": list(deployments.keys()),
            "result": deployment_result,
        }

    def start_automation(self, deployment_id: str):
        containers = self.get_container(deployment_id)

        if not containers:
            raise HTTPException(
                status_code=404,
                detail=f"No container found for deployment ID: {deployment_id}",
            )

        # Restart the container
        container = containers[0]
        container.start()

        return {
            "status": "success",
            "message": f"Container for deployment {deployment_id} started successfully",
        }

    def stop_automation(self, deployment_id: str):
        containers = self.get_container(deployment_id)

        if not containers:
            raise HTTPException(
                status_code=404,
                detail=f"No container found for deployment ID: {deployment_id}",
            )

        container = containers[0]
        container.stop()

        return {
            "status": "success",
            "message": f"Container for deployment {deployment_id} stopped successfully",
        }

    def restart_automation(self, deployment_id: str):
        containers = self.get_container(deployment_id)

        if not containers:
            raise HTTPException(
                status_code=404,
                detail=f"No container found for deployment ID: {deployment_id}",
            )

        # Restart the container
        container = containers[0]
        container.restart()

        return {
            "status": "success",
            "message": f"Container for deployment {deployment_id} restarted successfully",
        }

    async def activate_automation(self, deployment_id: str):
        bs_yaml = read_bitswan_yaml(self.gitops_dir)
        bs_yaml["deployments"][deployment_id]["active"] = True
        with open(os.path.join(self.gitops_dir, "bitswan.yaml"), "w") as f:
            yaml.dump(bs_yaml, f)

        # update git
        await update_git(
            self.gitops_dir, self.gitops_dir_host, deployment_id, "activate"
        )

        result = await self.deploy_automation(deployment_id)

        return result

    async def deactivate_automation(self, deployment_id: str):
        bs_yaml = read_bitswan_yaml(self.gitops_dir)
        bs_yaml["deployments"][deployment_id]["active"] = False
        with open(os.path.join(self.gitops_dir, "bitswan.yaml"), "w") as f:
            yaml.dump(bs_yaml, f)

        # update git
        await update_git(
            self.gitops_dir, self.gitops_dir_host, deployment_id, "deactivate"
        )

        self.remove_automation(deployment_id)

        return {
            "status": "success",
            "message": f"Deployment {deployment_id} deactivated successfully",
        }

    def get_automation_logs(self, deployment_id: str, lines: int = 100):
        containers = self.get_container(deployment_id)

        if not containers:
            raise HTTPException(
                status_code=404,
                detail=f"No container found for deployment ID: {deployment_id}",
            )

        container = containers[0]
        logs = container.logs(tail=lines)
        logs = logs.decode("utf-8")

        return {"status": "success", "logs": logs.split("\n")}

    def remove_automation(self, deployment_id: str):
        containers = self.get_container(deployment_id)

        if not containers:
            raise HTTPException(
                status_code=404,
                detail=f"No container found for deployment ID: {deployment_id}",
            )

        container = containers[0]
        container.stop()
        container.remove()

        return {
            "status": "success",
            "message": f"Container for deployment {deployment_id} removed successfully",
        }

    def generate_docker_compose(self, bs_yaml: dict):
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
            source_dir = os.path.join(self.gitops_dir, source)

            if not os.path.exists(source_dir):
                raise HTTPException(
                    status_code=500,
                    detail=f"Deployment directory {source_dir} does not exist",
                )
            else:
                pipeline_conf = read_pipeline_conf(source_dir)

            entry["environment"] = {"DEPLOYMENT_ID": deployment_id}
            entry["container_name"] = f"{self.get_workspace_name()}__{deployment_id}"
            entry["restart"] = "always"
            entry["labels"] = {
                "gitops.deployment_id": deployment_id,
                "gitops.workspace": self.get_workspace_name(),
            }
            entry["image"] = "bitswan/pipeline-runtime-environment:latest"

            network_mode = None
            secret_groups = []
            if pipeline_conf:
                network_mode = pipeline_conf.get(
                    "docker.compose", "network_mode", fallback=conf.get("network_mode")
                )
                secret_groups = pipeline_conf.get(
                    "secrets", "groups", fallback=""
                ).split(" ")
            for secret_group in secret_groups:
                # Skip empty secret groups
                if not secret_group:
                    continue
                if os.path.exists(self.secrets_dir):
                    secret_env_file = os.path.join(self.secrets_dir, secret_group)
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

            deployment_dir = os.path.join(self.gitops_dir_host, source)

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
                        raise HTTPException(
                            status_code=500, detail="Error adding route to Caddy"
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
        return dc_yaml
