import os
import shutil
from tempfile import NamedTemporaryFile
import zipfile
import docker
import yaml
import requests
from datetime import datetime
from app.models import DeployedAutomation
from app.utils import (
    add_workspace_route_to_caddy,
    calculate_checksum,
    calculate_deployment_checksum,
    calculate_uptime,
    docker_compose_up,
    generate_workspace_url,
    read_bitswan_yaml,
    read_pipeline_conf,
    remove_route_from_caddy,
    update_git,
    call_git_command,
    copy_worktree,
)
from app.services.image_service import ImageService
from fastapi import UploadFile, HTTPException


class AutomationService:
    def __init__(self):
        self.bs_home = os.environ.get("BITSWAN_GITOPS_DIR", "/mnt/repo/pipeline")
        self.bs_home_host = os.environ.get(
            "BITSWAN_GITOPS_DIR_HOST", "/home/root/.config/bitswan/local-gitops/"
        )
        self.workspace_id = os.environ.get("BITSWAN_WORKSPACE_ID")
        self.workspace_name = os.environ.get(
            "BITSWAN_WORKSPACE_NAME", "workspace-local"
        )
        self.aoc_url = os.environ.get("BITSWAN_AOC_URL")
        self.aoc_token = os.environ.get("BITSWAN_AOC_TOKEN")
        self.docker_client = docker.from_env()
        self.gitops_dir = os.path.join(self.bs_home, "gitops")
        self.gitops_dir_host = os.path.join(self.bs_home_host, "gitops")
        self.secrets_dir = os.path.join(self.bs_home, "secrets")

    def get_container(self, deployment_id):
        return self.docker_client.containers.list(
            all=True,  # Include stopped containers
            filters={
                "label": [
                    f"gitops.deployment_id={deployment_id}",
                    f"gitops.workspace={self.workspace_name}",
                ]
            },
        )

    def get_containers(self):
        return self.docker_client.containers.list(
            all=True,  # Include stopped containers
            filters={
                "label": [
                    "gitops.deployment_id",
                    f"gitops.workspace={self.workspace_name}",
                ]
            },
        )

    def get_automations(self):
        bs_yaml = read_bitswan_yaml(self.gitops_dir)

        if not bs_yaml:
            return []

        pres = {}
        for deployment_id, deployment_config in bs_yaml["deployments"].items():
            # Get the active stage
            active_stage = deployment_config.get("active_stage", "testing")
            
            # Get stage information
            stages_info = {}
            if "stages" in deployment_config:
                for stage, stage_config in deployment_config["stages"].items():
                    stages_info[stage] = {
                        "checksum": stage_config.get("checksum"),
                        "active": stage_config.get("active", False),
                        "deployed_at": stage_config.get("deployed_at"),
                        "promoted_from": stage_config.get("promoted_from")
                    }
            
            pres[deployment_id] = DeployedAutomation(
                container_id=None,
                endpoint_name=None,
                created_at=None,
                name=deployment_id,
                state=None,
                status=None,
                deployment_id=deployment_id,
                active=deployment_config.get("active", False),
                automation_url=None,
                relative_path=deployment_config.get("relative_path", None),
                # Add stage information as additional attributes
                active_stage=active_stage,
                stages=stages_info
            )

        info = self.docker_client.info()
        containers: list[docker.models.containers.Container] = self.get_containers()

        gitops_domain = os.environ.get("BITSWAN_GITOPS_DOMAIN", None)

        # updated pres with active containers
        for container in containers:
            deployment_id = container.labels["gitops.deployment_id"]
            if deployment_id in pres:
                label = container.attrs["Config"]["Labels"].get(
                    "gitops.intended_exposed", "false"
                )

                url = generate_workspace_url(
                    self.workspace_name, deployment_id, gitops_domain, True
                )

                if label != "true":
                    url = None

                # Preserve the stage information when updating with container info
                original_pres = pres[deployment_id]
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
                    active=original_pres.active,
                    automation_url=url,
                    relative_path=original_pres.relative_path,
                    # Preserve stage information
                    active_stage=getattr(original_pres, 'active_stage', None),
                    stages=getattr(original_pres, 'stages', {})
                )

        return list(pres.values())

    async def create_automation(
        self, deployment_id: str, file: UploadFile, relative_path: str = None, stage: str = "testing"
    ):
        with NamedTemporaryFile(delete=False) as temp_file:
            content = await file.read()
            temp_file.write(content)

            temp_file.close()
            # Use new checksum calculation based on deployment_id and stage
            checksum = calculate_checksum(temp_file.name)
            output_dir = f"{checksum}"

            try:
                bitswan_yaml_path = os.path.join(self.gitops_dir, "bitswan.yaml")

                output_dir = os.path.join(self.gitops_dir, output_dir)

                os.makedirs(output_dir, exist_ok=True)
                with zipfile.ZipFile(temp_file.name, "r") as zip_ref:
                    zip_ref.extractall(output_dir)

                # Update or create bitswan.yaml
                data = read_bitswan_yaml(self.gitops_dir)

                data = data or {"deployments": {}}
                deployments = data["deployments"]  # should never raise KeyError

                # Initialize deployment if it doesn't exist
                if deployment_id not in deployments:
                    deployments[deployment_id] = {}

                # Initialize stages if it doesn't exist
                if "stages" not in deployments[deployment_id]:
                    deployments[deployment_id]["stages"] = {}

                # Set up the stage
                deployments[deployment_id]["stages"][stage] = {
                    "checksum": checksum,
                    "active": True,
                    "deployed_at": datetime.now().isoformat()
                }

                # Set the current active stage (testing by default for new deployments)
                deployments[deployment_id]["active_stage"] = stage
                deployments[deployment_id]["active"] = True

                if relative_path:
                    deployments[deployment_id]["relative_path"] = relative_path

                data["deployments"] = deployments
                with open(bitswan_yaml_path, "w") as f:
                    yaml.dump(data, f)

                await update_git(
                    self.gitops_dir, self.gitops_dir_host, deployment_id, "create"
                )

                # add this file to git
                await call_git_command("git", "add", f"{checksum}", cwd=self.gitops_dir)

                # commit the changes
                await call_git_command(
                    "git",
                    "commit",
                    "-m",
                    f"Add {deployment_id} to bitswan.yaml (stage: {stage})",
                    cwd=self.gitops_dir,
                )

                # push the changes
                await call_git_command("git", "push", cwd=self.gitops_dir)

                return {
                    "message": "File processed successfully",
                    "output_directory": output_dir,
                    "checksum": checksum,
                    "stage": stage,
                }
            except Exception as e:
                return {"error": f"Error processing file: {str(e)}"}
            finally:
                os.unlink(temp_file.name)

    async def delete_automation(self, deployment_id: str):
        await self.remove_automation_from_bitswan(deployment_id)

        await update_git(self.gitops_dir, self.gitops_dir_host, deployment_id, "delete")
        result = remove_route_from_caddy(deployment_id, self.workspace_name)

        if not result:
            message = f"Deployment {deployment_id} deleted successfully, but failed to remove route from Caddy"
        else:
            message = f"Deployment {deployment_id} deleted successfully"

        containers = self.get_container(deployment_id)
        if containers:
            self.remove_automation(deployment_id)
        return {"status": "success", "message": message}

    async def get_tag(self, deployed_image: str):
        expected_prefix = f"{deployed_image}:sha"
        image_obj = self.docker_client.images.get(deployed_image)
        for tag in image_obj.tags:
            if tag.startswith(expected_prefix):
                deployed_image_checksum_tag = tag[len(expected_prefix) :]
                return deployed_image_checksum_tag
        return None

    async def deploy_automation(self, deployment_id: str, file: UploadFile = None, relative_path: str = None):
        """
        Deploy automation - now handles both creating new automations and deploying existing ones.
        If file is provided, creates new automation in testing stage.
        If no file, deploys the currently active stage.
        """
        os.environ["COMPOSE_PROJECT_NAME"] = self.workspace_name
        
        # If file is provided, create the automation first (always goes to testing stage)
        if file:
            if not file.filename.endswith(".zip"):
                raise HTTPException(status_code=400, detail="File must be a ZIP archive")
            
            create_result = await self.create_automation(deployment_id, file, relative_path, "testing")
            if "error" in create_result:
                raise HTTPException(status_code=500, detail=create_result["error"])
        
        bs_yaml = read_bitswan_yaml(self.gitops_dir)

        if not bs_yaml:
            raise HTTPException(status_code=500, detail="Error reading bitswan.yaml")

        # Check if deployment exists
        if deployment_id not in bs_yaml.get("deployments", {}):
            raise HTTPException(status_code=404, detail=f"Deployment {deployment_id} not found")

        deployment_config = bs_yaml["deployments"][deployment_id]
        
        # Get the active stage
        active_stage = deployment_config.get("active_stage", "testing")
        
        # Get the checksum for the active stage
        if "stages" not in deployment_config or active_stage not in deployment_config["stages"]:
            raise HTTPException(status_code=404, detail=f"No active stage found for deployment {deployment_id}")
        
        stage_config = deployment_config["stages"][active_stage]
        checksum = stage_config["checksum"]
        
        # Update the deployment config to use the checksum for deployment
        deployment_config["checksum"] = checksum
        
        # Create a temporary bitswan.yaml with just this deployment for docker-compose generation
        temp_bs_yaml = {"deployments": {deployment_id: deployment_config}}
        
        dc_yaml = self.generate_docker_compose(temp_bs_yaml)
        dc_config = yaml.safe_load(dc_yaml)

        image_tag = None
        if deployment_id in dc_config.get("services", {}):
            deployed_image = dc_config["services"][deployment_id].get("image")
            image_tag = await self.get_tag(deployed_image)

        # deploy the automation
        deployment_result = await docker_compose_up(
            self.gitops_dir, dc_yaml, deployment_id
        )

        for result in deployment_result.values():
            if result["return_code"] != 0:
                raise HTTPException(status_code=500, detail="Error deploying services")

        if image_tag:
            bs_yaml = read_bitswan_yaml(self.gitops_dir)
            if (
                bs_yaml
                and "deployments" in bs_yaml
                and deployment_id in bs_yaml["deployments"]
            ):
                # Update the stage config with the image tag
                if "stages" in bs_yaml["deployments"][deployment_id]:
                    active_stage = bs_yaml["deployments"][deployment_id].get("active_stage", "testing")
                    if active_stage in bs_yaml["deployments"][deployment_id]["stages"]:
                        bs_yaml["deployments"][deployment_id]["stages"][active_stage]["tag_checksum"] = image_tag

                bitswan_yaml_path = os.path.join(self.gitops_dir, "bitswan.yaml")
                with open(bitswan_yaml_path, "w") as f:
                    yaml.dump(bs_yaml, f)

                await update_git(
                    self.gitops_dir, self.gitops_dir_host, deployment_id, "deploy"
                )

        return {
            "message": "Deployed services successfully",
            "deployment_id": deployment_id,
            "stage": active_stage,
            "checksum": checksum,
            "result": deployment_result,
        }

    async def promote_automation(self, deployment_id: str, checksum: str):
        """
        Promote an automation from one stage to the next.
        Flow: testing -> staging -> production
        """
        bs_yaml = read_bitswan_yaml(self.gitops_dir)
        
        if not bs_yaml or deployment_id not in bs_yaml.get("deployments", {}):
            raise HTTPException(status_code=404, detail=f"Deployment {deployment_id} not found")
        
        deployment_config = bs_yaml["deployments"][deployment_id]
        
        if "stages" not in deployment_config:
            raise HTTPException(status_code=400, detail=f"No stages found for deployment {deployment_id}")
        
        # Find which stage contains this checksum
        source_stage = None
        for stage, stage_config in deployment_config["stages"].items():
            if stage_config.get("checksum") == checksum:
                source_stage = stage
                break
        
        if not source_stage:
            raise HTTPException(status_code=404, detail=f"Checksum {checksum} not found in any stage for deployment {deployment_id}")
        
        # Determine the target stage
        stage_progression = {"testing": "staging", "staging": "production"}
        target_stage = stage_progression.get(source_stage)
        
        if not target_stage:
            raise HTTPException(status_code=400, detail=f"Cannot promote from {source_stage} stage")
        
        # Check if target stage already exists and has a different checksum
        if target_stage in deployment_config["stages"]:
            existing_checksum = deployment_config["stages"][target_stage].get("checksum")
            if existing_checksum != checksum:
                # Copy the source directory to the target checksum directory
                source_checksum = checksum
                target_checksum = calculate_deployment_checksum(deployment_id, target_stage)
                
                source_dir = os.path.join(self.gitops_dir, source_checksum)
                target_dir = os.path.join(self.gitops_dir, target_checksum)
                
                if os.path.exists(source_dir):
                    # Remove existing target directory if it exists
                    if os.path.exists(target_dir):
                        shutil.rmtree(target_dir)
                    
                    # Copy source to target
                    shutil.copytree(source_dir, target_dir)
                    
                    # Update the target stage with new checksum
                    deployment_config["stages"][target_stage] = {
                        "checksum": target_checksum,
                        "active": True,
                        "deployed_at": datetime.now().isoformat(),
                        "promoted_from": source_stage
                    }
                else:
                    raise HTTPException(status_code=500, detail=f"Source directory {source_dir} not found")
            else:
                # Same checksum, just update the stage info
                deployment_config["stages"][target_stage]["active"] = True
                deployment_config["stages"][target_stage]["promoted_at"] = datetime.now().isoformat()
                deployment_config["stages"][target_stage]["promoted_from"] = source_stage
        else:
            # Target stage doesn't exist, create it
            target_checksum = calculate_deployment_checksum(deployment_id, target_stage)
            
            source_dir = os.path.join(self.gitops_dir, checksum)
            target_dir = os.path.join(self.gitops_dir, target_checksum)
            
            if os.path.exists(source_dir):
                # Copy source to target
                shutil.copytree(source_dir, target_dir)
                
                # Create the target stage
                deployment_config["stages"][target_stage] = {
                    "checksum": target_checksum,
                    "active": True,
                    "deployed_at": datetime.now().isoformat(),
                    "promoted_from": source_stage
                }
            else:
                raise HTTPException(status_code=500, detail=f"Source directory {source_dir} not found")
        
        # Update the active stage to the target stage
        deployment_config["active_stage"] = target_stage
        
        # Update bitswan.yaml
        bitswan_yaml_path = os.path.join(self.gitops_dir, "bitswan.yaml")
        with open(bitswan_yaml_path, "w") as f:
            yaml.dump(bs_yaml, f)
        
        # Add the new directory to git
        target_checksum = deployment_config["stages"][target_stage]["checksum"]
        await call_git_command("git", "add", f"{target_checksum}", cwd=self.gitops_dir)
        
        # Commit the changes
        await call_git_command(
            "git",
            "commit",
            "-m",
            f"Promote {deployment_id} from {source_stage} to {target_stage}",
            cwd=self.gitops_dir,
        )
        
        # Push the changes
        await call_git_command("git", "push", cwd=self.gitops_dir)
        
        return {
            "message": f"Successfully promoted {deployment_id} from {source_stage} to {target_stage}",
            "deployment_id": deployment_id,
            "source_stage": source_stage,
            "target_stage": target_stage,
            "checksum": target_checksum,
        }

    async def deploy_automations(self):
        os.environ["COMPOSE_PROJECT_NAME"] = self.workspace_name
        bs_yaml = read_bitswan_yaml(self.gitops_dir)

        if not bs_yaml:
            raise HTTPException(status_code=500, detail="Error reading bitswan.yaml")

        active_deployments = self.get_active_automations()

        filtered_bs_yaml = {"deployments": active_deployments}

        dc_yaml = self.generate_docker_compose(filtered_bs_yaml)
        deployments = active_deployments

        deployment_result = await docker_compose_up(self.gitops_dir, dc_yaml)

        for result in deployment_result.values():
            if result["return_code"] != 0:
                print(result["stdout"])
                print(result["stderr"])
                raise HTTPException(
                    status_code=500,
                    detail=f"Error deploying services: \nstdout:\n {result['stdout']}\nstderr:\n{result['stderr']}\n",
                )
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

    async def mark_as_inactive(self, deployment_id: str):
        """
        Mark the automation as inactive in bitswan.yaml
        and update git
        """
        bs_yaml = read_bitswan_yaml(self.gitops_dir)
        bs_yaml["deployments"][deployment_id]["active"] = False
        with open(os.path.join(self.gitops_dir, "bitswan.yaml"), "w") as f:
            yaml.dump(bs_yaml, f)
        await update_git(
            self.gitops_dir, self.gitops_dir_host, deployment_id, "mark_as_inactive"
        )

    async def mark_as_active(self, deployment_id: str):
        """
        Mark the automation as active in bitswan.yaml
        and update git
        """
        bs_yaml = read_bitswan_yaml(self.gitops_dir)
        bs_yaml["deployments"][deployment_id]["active"] = True
        with open(os.path.join(self.gitops_dir, "bitswan.yaml"), "w") as f:
            yaml.dump(bs_yaml, f)
        await update_git(
            self.gitops_dir, self.gitops_dir_host, deployment_id, "mark_as_active"
        )

    async def remove_automation_from_bitswan(self, deployment_id: str):
        """
        Remove the automation from bitswan.yaml
        and update git
        """
        bs_yaml = read_bitswan_yaml(self.gitops_dir)

        if deployment_id not in bs_yaml["deployments"]:
            return

        bs_yaml["deployments"].pop(deployment_id)
        with open(os.path.join(self.gitops_dir, "bitswan.yaml"), "w") as f:
            yaml.dump(bs_yaml, f)
        await update_git(self.gitops_dir, self.gitops_dir_host, deployment_id, "remove")

    # get active automations from bitswan.yaml
    def get_active_automations(self):
        """
        Get the active automations from bitswan.yaml
        """
        bs_yaml = read_bitswan_yaml(self.gitops_dir)
        active_deployments = {}
        for deployment_id, config in bs_yaml["deployments"].items():
            if config.get("active", False):
                # For new stage-based structure, include the active stage's checksum
                if "stages" in config and config.get("active_stage"):
                    active_stage = config.get("active_stage", "testing")
                    stage_config = config["stages"].get(active_stage, {})
                    if stage_config.get("checksum"):
                        # Create a config with the active stage's checksum
                        active_config = config.copy()
                        active_config["checksum"] = stage_config["checksum"]
                        active_deployments[deployment_id] = active_config
                    else:
                        active_deployments[deployment_id] = config
                else:
                    # Legacy structure
                    active_deployments[deployment_id] = config
        return active_deployments

    async def stop_automation(self, deployment_id: str):
        containers = self.get_container(deployment_id)

        if not containers:
            raise HTTPException(
                status_code=404,
                detail=f"No container found for deployment ID: {deployment_id}",
            )

        container = containers[0]
        container.stop()

        await self.mark_as_inactive(deployment_id)

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
        await self.mark_as_active(deployment_id)

        # update git
        await update_git(
            self.gitops_dir, self.gitops_dir_host, deployment_id, "activate"
        )

        result = await self.deploy_automation(deployment_id)

        return result

    async def deactivate_automation(self, deployment_id: str):
        await self.mark_as_inactive(deployment_id)

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

    async def remove_automation(self, deployment_id: str):
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

    async def pull_and_deploy(self, branch_name: str):
        await copy_worktree(branch_name)

        bs_yaml = read_bitswan_yaml(self.gitops_dir)
        if not bs_yaml or "deployments" not in bs_yaml:
            raise HTTPException(
                status_code=404, detail="No deployments found in bitswan.yaml"
            )

        active_deployments = self.get_active_automations()

        for deployment_id, config in active_deployments.items():
            tag_checksum = config.get("tag_checksum")
            if not tag_checksum:
                continue

            images_dir = os.path.join(self.gitops_dir, "images", tag_checksum)
            if not os.path.exists(images_dir):
                continue

            image_service = ImageService()
            await image_service.create_image(
                image_tag=deployment_id,
                build_context_path=images_dir,
                checksum=tag_checksum,
            )

        await self.deploy_automations()

        return {
            "status": "success",
            "message": f"Successfully synced branch {branch_name} and processed automations",
        }

    def get_emqx_jwt_token(self, deployment_id: str):
        if not self.workspace_id:
            raise HTTPException(
                status_code=500,
                detail=f"Workspace {self.workspace_name} is missing an ID",
            )
        url = f"{self.aoc_url}/api/automation_server/workspaces/{self.workspace_id}/pipelines/{deployment_id}/emqx/jwt"
        headers = {"Authorization": f"Bearer {self.aoc_token}"}
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            error_detail = f"AOC API error: {response.status_code} - {response.text}"
            print(f"JWT Token generation failed: {error_detail}")
            return None
        return response.json()

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

            # Handle both old and new structure
            if "stages" in conf and conf.get("active_stage"):
                # New stage-based structure
                active_stage = conf.get("active_stage", "testing")
                stage_config = conf["stages"].get(active_stage, {})
                source = stage_config.get("checksum") or deployment_id
            else:
                # Legacy structure
                source = conf.get("source") or conf.get("checksum") or deployment_id
            
            source_dir = os.path.join(self.gitops_dir, source)

            if not os.path.exists(source_dir):
                raise HTTPException(
                    status_code=500,
                    detail=f"Deployment directory {source_dir} does not exist",
                )
            else:
                pipeline_conf = read_pipeline_conf(source_dir)

            if self.workspace_id and self.aoc_url and self.aoc_token:
                # generate jwt token for automation
                jwt_token_response = self.get_emqx_jwt_token(deployment_id)
                if jwt_token_response is not None:
                    jwt_token = jwt_token_response.get("token")
                    emqx_url = jwt_token_response.get("url")
                    entry["environment"] = {
                        "MQTT_USERNAME": deployment_id,
                        "MQTT_PASSWORD": jwt_token,
                        "MQTT_BROKER_URL": emqx_url,
                        "DEPLOYMENT_ID": deployment_id,
                    }
            else:
                entry["environment"] = {"DEPLOYMENT_ID": deployment_id}
            entry["container_name"] = f"{self.workspace_name}__{deployment_id}"
            entry["restart"] = "always"
            entry["labels"] = {
                "gitops.deployment_id": deployment_id,
                "gitops.workspace": self.workspace_name,
                "gitops.intended_exposed": "false",
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
                    result = add_workspace_route_to_caddy(deployment_id, port)
                    entry["labels"]["gitops.intended_exposed"] = "true"
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
