import os
import json
from tempfile import NamedTemporaryFile
import zipfile
import docker
import yaml
import requests
from datetime import datetime
from typing import Callable
from app.models import DeployedAutomation
from app.utils import (
    add_workspace_route_to_caddy,
    calculate_git_tree_hash,
    calculate_uptime,
    docker_compose_up,
    generate_workspace_url,
    read_bitswan_yaml,
    read_pipeline_conf,
    read_automation_config,
    remove_route_from_caddy,
    update_git,
    call_git_command,
    call_git_command_with_output,
    copy_worktree,
)
from app.services.image_service import ImageService
from fastapi import UploadFile, HTTPException
import docker.errors


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
        self.gitops_domain = os.environ.get("BITSWAN_GITOPS_DOMAIN")
        self.workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME")
        self.oauth2_proxy_path = os.environ.get("OAUTH2_PROXY_PATH")
        self.oauth2_proxy_port = 9999
        self.docker_client = docker.from_env()
        self.gitops_dir = os.path.join(self.bs_home, "gitops")
        self.gitops_dir_host = os.path.join(self.bs_home_host, "gitops")
        self.secrets_dir = os.path.join(self.bs_home, "secrets")
        # Workspace directory for live-dev mode (source code mounting)
        # Uses same path structure as jupyter_service for consistency
        self.workspace_dir = os.path.join(self.bs_home, "workspace")
        self.workspace_dir_host = os.path.join(self.bs_home_host, "workspace")
        # Cache for automation history: key is (deployment_id, page, page_size), value is (commit_hash, response)
        self._history_cache: dict[tuple[str, int, int], tuple[str, dict]] = {}

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
                automation_url=None,
                relative_path=bs_yaml["deployments"][deployment_id].get(
                    "relative_path", None
                ),
                stage=bs_yaml["deployments"][deployment_id].get("stage", "production"),
                version_hash=bs_yaml["deployments"][deployment_id].get(
                    "checksum", None
                ),
            )
            for deployment_id in bs_yaml["deployments"]
        }

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
                    automation_url=url,
                    relative_path=pres[deployment_id].relative_path,
                    stage=pres[deployment_id].stage,
                    version_hash=pres[deployment_id].version_hash,
                )

        return list(pres.values())

    async def _upload_and_commit_asset(
        self,
        file: UploadFile,
        commit_message: str | Callable[[str], str],
        checksum: str,
    ) -> dict:
        """
        Shared logic for uploading an asset (zip file), unpacking it,
        and committing it to git. Returns dict with checksum and output_directory.

        commit_message can be a string or a callable that takes checksum and returns a string.
        checksum: Pre-calculated git tree hash that will be verified.
        """
        with NamedTemporaryFile(delete=False) as temp_file:
            content = await file.read()
            temp_file.write(content)

            temp_file.close()
            output_dir = f"{checksum}"

            try:
                output_dir = os.path.join(self.gitops_dir, output_dir)

                os.makedirs(output_dir, exist_ok=True)
                with zipfile.ZipFile(temp_file.name, "r") as zip_ref:
                    zip_ref.extractall(output_dir)

                # Verify the checksum using git tree hash algorithm
                calculated_hash = await calculate_git_tree_hash(output_dir)
                if calculated_hash != checksum:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Checksum verification failed. Expected {checksum}, got {calculated_hash}",
                    )

                # Generate commit message if it's a callable
                if callable(commit_message):
                    final_commit_message = commit_message(checksum)
                else:
                    final_commit_message = commit_message

                # add this file to git
                await call_git_command("git", "add", f"{checksum}", cwd=self.gitops_dir)

                # commit the changes
                await call_git_command(
                    "git",
                    "commit",
                    "-m",
                    final_commit_message,
                    cwd=self.gitops_dir,
                )

                # push the changes
                await call_git_command("git", "push", cwd=self.gitops_dir)

                return {
                    "checksum": checksum,
                    "output_directory": output_dir,
                }
            except Exception as e:
                raise Exception(f"Error processing file: {str(e)}")
            finally:
                os.unlink(temp_file.name)

    async def create_automation(
        self,
        deployment_id: str,
        file: UploadFile,
        relative_path: str = None,
        checksum: str = None,
    ):
        if not checksum:
            raise HTTPException(status_code=400, detail="Checksum is required")
        result = await self._upload_and_commit_asset(
            file, f"Add {deployment_id} to bitswan.yaml", checksum=checksum
        )
        checksum = result["checksum"]
        output_dir = result["output_directory"]

        try:
            bitswan_yaml_path = os.path.join(self.gitops_dir, "bitswan.yaml")

            # Update or create bitswan.yaml
            data = read_bitswan_yaml(self.gitops_dir)

            data = data or {"deployments": {}}
            deployments = data["deployments"]  # should never raise KeyError

            deployments[deployment_id] = deployments.get(deployment_id, {})
            deployments[deployment_id]["checksum"] = checksum
            deployments[deployment_id]["active"] = True

            if relative_path:
                deployments[deployment_id]["relative_path"] = relative_path

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
            return {"error": f"Error processing file: {str(e)}"}

    async def upload_asset(self, file: UploadFile, checksum: str):
        """
        Upload an asset (zip file), unpack it, and return the checksum.
        Similar to create_automation but without deployment_id.

        checksum: Pre-calculated git tree hash that will be verified.
        """
        try:
            result = await self._upload_and_commit_asset(
                file, lambda checksum: f"Add asset {checksum}", checksum=checksum
            )
            return {
                "message": "Asset uploaded successfully",
                "output_directory": result["output_directory"],
                "checksum": result["checksum"],
            }
        except Exception as e:
            return {"error": f"Error processing file: {str(e)}"}

    def list_assets(self):
        """
        List all assets (checksum directories) in the gitops directory.
        """
        assets = []
        if not os.path.exists(self.gitops_dir):
            return assets

        for item in os.listdir(self.gitops_dir):
            item_path = os.path.join(self.gitops_dir, item)
            # Check if it's a directory and looks like a checksum (hex string, typically 40 chars for SHA1)
            if (
                os.path.isdir(item_path)
                and (len(item) == 40 or len(item) == 64)
                and all(c in "0123456789abcdef" for c in item.lower())
            ):
                assets.append(
                    {
                        "checksum": item,
                        "path": item_path,
                        "exists": os.path.exists(item_path),
                    }
                )
        return assets

    async def _get_latest_commit_hash(self, bitswan_dir: str) -> str:
        """
        Get the latest commit hash (HEAD) from the git repository.
        """
        stdout, stderr, return_code = await call_git_command_with_output(
            "git",
            "rev-parse",
            "HEAD",
            cwd=bitswan_dir,
        )
        if return_code != 0:
            raise HTTPException(
                status_code=500, detail=f"Error getting latest commit hash: {stderr}"
            )
        return stdout.strip()

    async def get_automation_history(
        self, deployment_id: str, page: int = 1, page_size: int = 20
    ):
        """
        Get paginated history of automation changes from git.
        Only includes entries where there are actual changes to the automation.
        Cached responses are invalidated when the commit hash changes.
        """

        # Get git log for bitswan.yaml file
        # Use git log to get commits that modified bitswan.yaml
        # Then parse each commit to see if it affected the deployment_id

        host_path = os.environ.get("HOST_PATH")
        if host_path:
            bitswan_dir = self.gitops_dir_host
        else:
            bitswan_dir = self.gitops_dir

        # Get the latest commit hash
        current_commit_hash = await self._get_latest_commit_hash(bitswan_dir)

        # Check cache
        cache_key = (deployment_id, page, page_size)
        if cache_key in self._history_cache:
            cached_commit_hash, cached_response = self._history_cache[cache_key]
            # If commit hash matches, return cached response
            if cached_commit_hash == current_commit_hash:
                return cached_response
            # If commit hash changed, invalidate all caches
            else:
                self._history_cache.clear()

        # Get git log for bitswan.yaml file
        # Use git log to get commits that modified bitswan.yaml
        # Then parse each commit to see if it affected the deployment_id

        # Get commits that modified bitswan.yaml
        log_format = '{"commit": "%H", "author": "%an", "date": "%ai", "message": "%s"}'
        stdout, stderr, return_code = await call_git_command_with_output(
            "git",
            "log",
            "--format=" + log_format,
            "--date=iso",
            "--",
            "bitswan.yaml",
            cwd=bitswan_dir,
        )

        if return_code != 0:
            raise HTTPException(
                status_code=500, detail=f"Error getting git history: {stderr}"
            )

        commits = []
        for line in stdout.strip().split("\n"):
            if not line.strip():
                continue
            try:
                commit_data = json.loads(line)
                commits.append(commit_data)
            except json.JSONDecodeError:
                continue

        # Now check each commit to see if it actually changed the deployment_id
        history_entries = []
        previous_checksum = None

        for commit in commits:
            commit_hash = commit["commit"]

            # Get the bitswan.yaml content at this commit
            stdout, stderr, return_code = await call_git_command_with_output(
                "git",
                "show",
                f"{commit_hash}:bitswan.yaml",
                cwd=bitswan_dir,
            )

            if return_code != 0:
                # File might not exist at this commit, skip
                continue

            try:
                commit_yaml = yaml.safe_load(stdout)
                if not commit_yaml or "deployments" not in commit_yaml:
                    continue

                deployment_config = commit_yaml.get("deployments", {}).get(
                    deployment_id
                )

                # Only add entry if there's a checksum and it's different from the previous one
                if deployment_config is None:
                    # Deployment doesn't exist in this commit, skip
                    continue

                current_checksum = deployment_config.get("checksum")

                # Only add entry if checksum exists and is different from previous checksum
                if current_checksum and current_checksum != previous_checksum:
                    entry = {
                        "commit": commit_hash,
                        "author": commit["author"],
                        "date": commit["date"],
                        "message": commit["message"],
                        "checksum": current_checksum,
                        "stage": deployment_config.get("stage", "production"),
                        "relative_path": deployment_config.get("relative_path"),
                        "active": deployment_config.get("active"),
                        "tag_checksum": deployment_config.get("tag_checksum"),
                    }
                    history_entries.append(entry)
                    previous_checksum = current_checksum

            except yaml.YAMLError:
                continue

        # Paginate results
        total = len(history_entries)
        start = (page - 1) * page_size
        end = start + page_size
        paginated_entries = history_entries[start:end]

        response = {
            "items": paginated_entries,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
        }

        # Cache the response with the current commit hash
        cache_key = (deployment_id, page, page_size)
        self._history_cache[cache_key] = (current_commit_hash, response)

        return response

    def start_oauth2_proxy_in_container(self, deployment_id: str):
        """Start oauth2-proxy in a running container after deployment"""
        containers = self.get_container(deployment_id)

        container = containers[0]
        labels = container.labels

        # Check if oauth2 is enabled via labels
        if labels.get("gitops.oauth2.enabled") != "true":
            return False

        # Ensure container is running
        if container.status != "running":
            print(
                f"Warning: Container {container.name} is not running (status: {container.status}), cannot start oauth2-proxy"
            )
            return False

        upstream_url = labels.get("gitops.oauth2.upstream")
        if not upstream_url:
            print(
                f"Warning: No upstream URL found for oauth2-proxy in deployment {deployment_id}"
            )
            return False

        try:
            # Check if oauth2-proxy is already running to avoid duplicates
            check_result = container.exec_run(
                "pgrep -f oauth2-proxy || true",
                stdout=True,
                stderr=True,
            )
            if check_result.exit_code == 0 and check_result.output.strip():
                print(f"oauth2-proxy already running in container {container.name}")
                return True

            # Start oauth2-proxy in the background using sh -c to properly handle backgrounding
            print(
                f"Starting oauth2-proxy with upstream: {upstream_url} in container {container.name}"
            )
            # Use sh -c with proper backgrounding that returns immediately
            cmd = f"sh -c 'oauth2-proxy --upstream={upstream_url} > /tmp/oauth2-proxy.log 2>&1 &'"
            exec_result = container.exec_run(
                cmd,
                stdout=True,
                stderr=True,
                detach=False,
            )

            if exec_result.exit_code == 0:
                print(
                    f"Successfully started oauth2-proxy in container {container.name}"
                )
                return True
            else:
                error_msg = (
                    exec_result.output.decode("utf-8")
                    if exec_result.output
                    else "Unknown error"
                )
                print(
                    f"Failed to start oauth2-proxy in container {container.name}: {error_msg}"
                )
                return False

        except Exception as e:
            print(
                f"Exception while starting oauth2-proxy in container {container.name}: {str(e)}"
            )
            return False

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
            await self.remove_automation(deployment_id)
        return {"status": "success", "message": message}

    async def get_tag(self, deployed_image: str):
        expected_prefix = f"{deployed_image}:sha"
        try:
            image_obj = self.docker_client.images.get(deployed_image)
        except docker.errors.ImageNotFound:
            return None
        for tag in image_obj.tags:
            if tag.startswith(expected_prefix):
                deployed_image_checksum_tag = tag[len(expected_prefix) :]
                return deployed_image_checksum_tag
        return None

    async def deploy_automation(
        self,
        deployment_id: str,
        checksum: str | None = None,
        stage: str | None = None,
        relative_path: str | None = None,
    ):
        print(f"[deploy_automation] deployment_id={deployment_id}, checksum={checksum}, stage={stage}, relative_path={relative_path}")
        os.environ["COMPOSE_PROJECT_NAME"] = self.workspace_name
        bs_yaml = read_bitswan_yaml(self.gitops_dir)

        # Initialize bitswan.yaml if it doesn't exist
        if not bs_yaml:
            bs_yaml = {"deployments": {}}
            bitswan_yaml_path = os.path.join(self.gitops_dir, "bitswan.yaml")
            with open(bitswan_yaml_path, "w") as f:
                yaml.dump(bs_yaml, f)
            await update_git(
                self.gitops_dir, self.gitops_dir_host, deployment_id, "initialize"
            )

        # Update bitswan.yaml with new parameters if provided
        if checksum is not None or stage is not None or relative_path is not None:
            if deployment_id not in bs_yaml.get("deployments", {}):
                bs_yaml.setdefault("deployments", {})[deployment_id] = {}

            deployment_config = bs_yaml["deployments"][deployment_id]

            if checksum is not None:
                deployment_config["checksum"] = checksum

            if stage is not None:
                # Map production to empty string
                deployment_config["stage"] = "" if stage == "production" else stage

            if relative_path is not None:
                deployment_config["relative_path"] = relative_path

            # Set active to True by default when deploying (unless explicitly set to False)
            if "active" not in deployment_config:
                deployment_config["active"] = True

            bitswan_yaml_path = os.path.join(self.gitops_dir, "bitswan.yaml")
            with open(bitswan_yaml_path, "w") as f:
                yaml.dump(bs_yaml, f)

            await update_git(
                self.gitops_dir, self.gitops_dir_host, deployment_id, "deploy"
            )

            # Re-read to get updated config
            bs_yaml = read_bitswan_yaml(self.gitops_dir)

        print(f"[deploy_automation] Final bs_yaml deployments: {bs_yaml.get('deployments', {})}")
        print(f"[deploy_automation] workspace_dir={self.workspace_dir}, exists={os.path.exists(self.workspace_dir)}")
        if os.path.exists(self.workspace_dir):
            try:
                contents = os.listdir(self.workspace_dir)
                print(f"[deploy_automation] workspace_dir contents: {contents[:20]}")  # First 20 items
            except Exception as e:
                print(f"[deploy_automation] Error listing workspace_dir: {e}")
        dc_yaml = self.generate_docker_compose(bs_yaml)
        deployments = bs_yaml.get("deployments", {})

        dc_config = yaml.safe_load(dc_yaml)

        # deploy the automation
        deployment_result = await docker_compose_up(
            self.gitops_dir, dc_yaml, deployment_id
        )

        # record deployment in bitswan.yaml

        image_tag = None
        if deployment_id in dc_config.get("services", {}):
            deployed_image = dc_config["services"][deployment_id].get("image")
            image_tag = await self.get_tag(deployed_image)

        for result in deployment_result.values():
            if result["return_code"] != 0:
                raise HTTPException(
                    status_code=500,
                    detail=f"Error deploying services: \ndocker-compose:\n {dc_yaml}\n\nstdout:\n {result['stdout']}\nstderr:\n{result['stderr']}\n",
                )

        self.start_oauth2_proxy_in_container(deployment_id)

        if image_tag:
            bs_yaml = read_bitswan_yaml(self.gitops_dir)
            if (
                bs_yaml
                and "deployments" in bs_yaml
                and deployment_id in bs_yaml["deployments"]
            ):
                bs_yaml["deployments"][deployment_id]["tag_checksum"] = image_tag

                bitswan_yaml_path = os.path.join(self.gitops_dir, "bitswan.yaml")
                with open(bitswan_yaml_path, "w") as f:
                    yaml.dump(bs_yaml, f)

                await update_git(
                    self.gitops_dir, self.gitops_dir_host, deployment_id, "deploy"
                )

        return {
            "message": "Deployed services successfully",
            "deployments": list(deployments.get(deployment_id, {}).keys()),
            "result": deployment_result,
        }

    async def deploy_automations(self):
        os.environ["COMPOSE_PROJECT_NAME"] = self.workspace_name
        bs_yaml = read_bitswan_yaml(self.gitops_dir)

        # Initialize bitswan.yaml if it doesn't exist
        if not bs_yaml:
            bs_yaml = {"deployments": {}}
            bitswan_yaml_path = os.path.join(self.gitops_dir, "bitswan.yaml")
            with open(bitswan_yaml_path, "w") as f:
                yaml.dump(bs_yaml, f)
            await update_git(self.gitops_dir, self.gitops_dir_host, "all", "initialize")

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

        for deployment_id in deployments.keys():
            self.start_oauth2_proxy_in_container(deployment_id)

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

        self.start_oauth2_proxy_in_container(deployment_id)

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

        self.start_oauth2_proxy_in_container(deployment_id)

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
        image_tags = []

        for deployment_id, config in active_deployments.items():
            tag_checksum = config.get("tag_checksum")
            if not tag_checksum:
                continue

            images_root_dir = os.path.join(self.gitops_dir, "images", tag_checksum)
            source_dir = os.path.join(images_root_dir, "src")
            if not os.path.exists(source_dir):
                source_dir = images_root_dir

            if not os.path.exists(source_dir):
                continue

            image_service = ImageService()

            result = await image_service.create_image(
                image_tag=deployment_id,
                build_context_path=source_dir,
                checksum=tag_checksum,
            )
            image_tags.append(result["tag"])

        return {
            "status": "success",
            "message": f"Successfully synced branch {branch_name} and processed automations",
            "image_tags": image_tags,
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

    def add_keycloak_redirect_uri(self, redirect_uri: str):
        """Add a redirect URI to the workspace's Keycloak client"""
        if not self.workspace_id:
            print(
                f"Warning: Workspace {self.workspace_name} is missing an ID, skipping Keycloak redirect URI registration"
            )
            return None
        if not self.aoc_url or not self.aoc_token:
            print(
                "Warning: AOC URL or token not configured, skipping Keycloak redirect URI registration"
            )
            return None

        url = f"{self.aoc_url}/api/automation_server/workspaces/{self.workspace_id}/keycloak/add-redirect-uri/"

        headers = {
            "Authorization": f"Bearer {self.aoc_token}",
            "Content-Type": "application/json",
        }
        payload = {"redirect_uri": redirect_uri.strip()}

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            if response.status_code == 200:
                result = response.json()
                return result
            else:
                error_detail = (
                    f"Keycloak API error: {response.status_code} - {response.text}"
                )
                print(
                    f"Warning: Failed to add redirect URI to Keycloak: {error_detail}"
                )
                return None
        except Exception as e:
            print(f"Warning: Exception while adding redirect URI to Keycloak: {str(e)}")
            return None

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

            # For live-dev with relative_path, use workspace directory for config
            stage = conf.get("stage", "production")
            if stage == "":
                stage = "production"
            relative_path = conf.get("relative_path")

            if stage == "live-dev" and relative_path:
                # Read config from workspace source directory
                workspace_source_dir = os.path.join(self.workspace_dir, relative_path)
                print(f"[live-dev] {deployment_id}: Looking for automation config in: {workspace_source_dir}")
                if not os.path.exists(workspace_source_dir):
                    # Skip stale live-dev deployments where source no longer exists
                    print(f"[live-dev] {deployment_id}: Skipping - directory does not exist: {workspace_source_dir}")
                    continue
                print(f"[live-dev] {deployment_id}: Directory exists, reading config...")
                pipeline_conf = read_pipeline_conf(workspace_source_dir)
                automation_config = read_automation_config(workspace_source_dir)
                print(f"[live-dev] {deployment_id}: Config loaded: image={automation_config.image}, expose={automation_config.expose}")
            elif not os.path.exists(source_dir):
                raise HTTPException(
                    status_code=500,
                    detail=f"Deployment directory {source_dir} does not exist",
                )
            else:
                pipeline_conf = read_pipeline_conf(source_dir)
                automation_config = read_automation_config(source_dir)

            if self.workspace_id and self.aoc_url and self.aoc_token:
                # generate jwt token for automation
                # jwt_token_response = self.get_emqx_jwt_token(deployment_id)
                # if jwt_token_response is not None:
                #     jwt_token = jwt_token_response.get("token")
                #     emqx_url = jwt_token_response.get("url")
                #     entry["environment"] = {
                #         "MQTT_USERNAME": deployment_id,
                #         "MQTT_PASSWORD": jwt_token,
                #         "MQTT_BROKER_URL": emqx_url,
                #         "DEPLOYMENT_ID": deployment_id,
                #     }
                print("Skipping JWT token generation")
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

            # Set BITSWAN environment variables (stage already determined above)
            if "environment" not in entry:
                entry["environment"] = {}
            entry["environment"]["BITSWAN_AUTOMATION_STAGE"] = stage
            entry["environment"]["BITSWAN_DEPLOYMENT_ID"] = deployment_id
            if self.workspace_name:
                entry["environment"]["BITSWAN_WORKSPACE_NAME"] = self.workspace_name
            if self.gitops_domain:
                entry["environment"]["BITSWAN_GITOPS_DOMAIN"] = self.gitops_domain

            network_mode = None
            secret_groups = []

            # Get secret groups based on config format
            if automation_config.config_format == "toml":
                # For TOML format, use stage-specific secrets only (no fallback)
                # live-dev uses dev secrets
                if stage in ("dev", "live-dev") and automation_config.dev_groups:
                    secret_groups = automation_config.dev_groups
                elif stage == "staging" and automation_config.staging_groups:
                    secret_groups = automation_config.staging_groups
                elif stage == "production" and automation_config.production_groups:
                    secret_groups = automation_config.production_groups
            elif pipeline_conf:
                # For INI format (pipelines.conf), use existing logic with fallback
                network_mode = pipeline_conf.get(
                    "docker.compose", "network_mode", fallback=conf.get("network_mode")
                )

                # Check for stage-specific secret groups first, then fall back to groups
                stage_groups_key = f"{stage}_groups"
                secret_groups_str = pipeline_conf.get(
                    "secrets", stage_groups_key, fallback=""
                )
                if not secret_groups_str:
                    # Fall back to generic groups if stage-specific groups not set
                    secret_groups_str = pipeline_conf.get(
                        "secrets", "groups", fallback=""
                    )
                secret_groups = (
                    secret_groups_str.split(" ") if secret_groups_str else []
                )

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

            # Use unified automation config for image, expose, and port
            entry["image"] = automation_config.image
            expose = automation_config.expose
            port = automation_config.port
            expose_to_groups = automation_config.expose_to or []

            # Error if both expose and expose_to_groups are set
            if expose and expose_to_groups:
                raise HTTPException(
                    status_code=400,
                    detail="Cannot specify both 'expose' and 'expose_to'. Use 'expose_to' alone to enable exposure with OAuth2.",
                )

            # If expose_to_groups is set, automatically enable exposure
            if expose_to_groups:
                expose = True

            if expose and port:
                # Set URL env vars for exposed automations
                # These allow constructing URLs to any automation: {prefix}{deploymentId}{suffix}
                url_prefix = f"https://{self.workspace_name}-"
                url_suffix = f".{self.gitops_domain}"
                automation_url = f"{url_prefix}{deployment_id}{url_suffix}"

                entry["environment"]["BITSWAN_AUTOMATION_URL"] = automation_url
                entry["environment"]["BITSWAN_URL_PREFIX"] = url_prefix
                entry["environment"]["BITSWAN_URL_SUFFIX"] = url_suffix

                if expose_to_groups:
                    endpoint = automation_url
                    redirect_uri = f"{endpoint}/oauth2/callback"

                    # Register the redirect URI with Keycloak
                    self.add_keycloak_redirect_uri(redirect_uri)

                    oauth2_envs = {
                        k: v for k, v in os.environ.items() if k.startswith("OAUTH2")
                    }
                    oauth2_envs.update(
                        {
                            "OAUTH_ENABLED": "true",
                        }
                    )

                    oauth2_envs.update(
                        {
                            "OAUTH2_PROXY_UPSTREAM": f"http://127.0.0.1:{port}",
                            "OAUTH2_PROXY_REDIRECT_URL": redirect_uri,
                            "OAUTH2_PROXY_ALLOWED_GROUPS": ",".join(expose_to_groups),
                            "BITSWAN_AUTOMATION_URL": automation_url,
                        }
                    )
                    entry["environment"] = oauth2_envs
                    entry["volumes"] = [
                        f"{self.oauth2_proxy_path}:/usr/local/bin/oauth2-proxy:ro"
                    ]

                    # Store oauth2 config in labels for post-deployment execution
                    entry["labels"]["gitops.oauth2.enabled"] = "true"
                    entry["labels"]["gitops.oauth2.upstream"] = (
                        f"http://127.0.0.1:{port}"
                    )
                    entry["labels"]["gitops.intended_exposed"] = "true"
                    add_workspace_route_to_caddy(deployment_id, self.oauth2_proxy_port)

                else:
                    result = add_workspace_route_to_caddy(deployment_id, port)
                    entry["labels"]["gitops.intended_exposed"] = "true"
                    if not result:
                        raise HTTPException(
                            status_code=500, detail="Error adding route to Caddy"
                        )

            if "volumes" not in entry:
                entry["volumes"] = []

            # For live-dev stage, mount source from workspace (for live editing)
            # Otherwise, mount from gitops deployment directory (checksum dir)
            if stage == "live-dev" and relative_path:
                # Mount the original source code for live development
                # Uses same workspace path as jupyter_service
                source_mount_path = os.path.join(self.workspace_dir_host, relative_path)
                entry["volumes"].append(
                    f"{source_mount_path}:{automation_config.mount_path}"
                )
            else:
                entry["volumes"].append(
                    f"{deployment_dir}:{automation_config.mount_path}"
                )

            if conf.get("enabled", True):
                dc["services"][deployment_id] = entry

        dc["networks"] = {}
        for network in external_networks:
            dc["networks"][network] = {"external": True}
        dc_yaml = yaml.dump(dc)
        return dc_yaml
