import os
import json
from tempfile import NamedTemporaryFile
import zipfile
import yaml
import requests
from datetime import datetime
from typing import Callable
from app.models import DeployedAutomation
from app.utils import (
    add_workspace_route_to_caddy,
    AutomationConfig,
    calculate_git_tree_hash,
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
    GitLockContext,
)
from app.async_docker import get_async_docker_client, DockerError
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
        self.gitops_domain = os.environ.get("BITSWAN_GITOPS_DOMAIN")
        self.workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME")
        self.oauth2_proxy_path = os.environ.get("OAUTH2_PROXY_PATH")
        self.oauth2_proxy_port = 9999
        self.certs_dir_host = os.environ.get("BITSWAN_CERTS_DIR")
        self.gitops_dir = os.path.join(self.bs_home, "gitops")
        self.gitops_dir_host = os.path.join(self.bs_home_host, "gitops")
        self.secrets_dir = os.path.join(self.bs_home, "secrets")
        # Workspace directory for live-dev mode (source code mounting)
        # Uses same path structure as jupyter_service for consistency
        self.workspace_dir = os.path.join(self.bs_home, "workspace")
        self.workspace_dir_host = os.path.join(self.bs_home_host, "workspace")
        # Cache for automation history: key is (deployment_id, page, page_size), value is (commit_hash, response)
        self._history_cache: dict[tuple[str, int, int], tuple[str, dict]] = {}

    async def get_container(self, deployment_id) -> list[dict]:
        """Get containers for a specific deployment using async Docker client."""
        docker_client = get_async_docker_client()
        containers = await docker_client.list_containers(
            all=True,
            filters={
                "label": [
                    f"gitops.deployment_id={deployment_id}",
                    f"gitops.workspace={self.workspace_name}",
                ]
            },
        )
        return containers

    async def get_containers(self) -> list[dict]:
        """Get all gitops containers using async Docker client."""
        docker_client = get_async_docker_client()
        containers = await docker_client.list_containers(
            all=True,
            filters={
                "label": [
                    "gitops.deployment_id",
                    f"gitops.workspace={self.workspace_name}",
                ]
            },
        )
        return containers

    async def get_automations(self):
        """Get all automations with their container status using async Docker client."""
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

        docker_client = get_async_docker_client()
        info = await docker_client.info()
        containers = await self.get_containers()

        gitops_domain = os.environ.get("BITSWAN_GITOPS_DOMAIN", None)

        # updated pres with active containers
        for container in containers:
            labels = container.get("Labels", {})
            deployment_id = labels.get("gitops.deployment_id")
            if deployment_id and deployment_id in pres:
                label = labels.get("gitops.intended_exposed", "false")

                url = generate_workspace_url(
                    self.workspace_name, deployment_id, gitops_domain, True
                )

                if label != "true":
                    url = None

                # Parse created timestamp
                created_str = container.get("Created")
                created_at = None
                if created_str:
                    try:
                        # Docker API returns Unix timestamp
                        created_at = datetime.utcfromtimestamp(created_str)
                    except (ValueError, TypeError):
                        pass

                # Get container state
                state = container.get("State", "unknown")
                status = container.get("Status", "")

                pres[deployment_id] = DeployedAutomation(
                    container_id=container.get("Id"),
                    endpoint_name=info.get("Name"),
                    created_at=created_at,
                    name=deployment_id,
                    state=state,
                    status=status,
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

        Uses async git lock to prevent race conditions with concurrent uploads.
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

                # Use async lock for git operations to prevent race conditions
                async with GitLockContext(timeout=10.0):
                    # add this file to git
                    await call_git_command(
                        "git", "add", f"{checksum}", cwd=self.gitops_dir
                    )

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

    async def upload_asset_from_path(self, file_path: str, checksum: str):
        """
        Upload an asset from a file path (for streaming uploads).
        Similar to upload_asset but takes a file path instead of UploadFile.

        checksum: Pre-calculated git tree hash that will be verified.
        """
        output_dir = os.path.join(self.gitops_dir, checksum)
        os.makedirs(output_dir, exist_ok=True)

        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(output_dir)

        # Verify the checksum using git tree hash algorithm
        calculated_hash = await calculate_git_tree_hash(output_dir)
        if calculated_hash != checksum:
            raise HTTPException(
                status_code=400,
                detail=f"Checksum verification failed. Expected {checksum}, got {calculated_hash}",
            )

        # Use async lock for git operations to prevent race conditions
        async with GitLockContext(timeout=10.0):
            await call_git_command("git", "add", f"{checksum}", cwd=self.gitops_dir)
            await call_git_command(
                "git",
                "commit",
                "-m",
                f"Add asset {checksum}",
                cwd=self.gitops_dir,
            )
            await call_git_command("git", "push", cwd=self.gitops_dir)

        return {
            "message": "Asset uploaded successfully",
            "output_directory": output_dir,
            "checksum": checksum,
        }

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

    async def start_oauth2_proxy_in_container(self, deployment_id: str):
        """Start oauth2-proxy in a running container after deployment"""
        containers = await self.get_container(deployment_id)

        if not containers:
            return False

        container = containers[0]
        container_id = container.get("Id")
        labels = container.get("Labels", {})
        container_name = container.get("Names", [deployment_id])[0].lstrip("/")

        # Check if oauth2 is enabled via labels
        if labels.get("gitops.oauth2.enabled") != "true":
            return False

        # Ensure container is running
        state = container.get("State", "")
        if state != "running":
            print(
                f"Warning: Container {container_name} is not running (status: {state}), cannot start oauth2-proxy"
            )
            return False

        upstream_url = labels.get("gitops.oauth2.upstream")
        if not upstream_url:
            print(
                f"Warning: No upstream URL found for oauth2-proxy in deployment {deployment_id}"
            )
            return False

        try:
            docker_client = get_async_docker_client()

            # Check if oauth2-proxy is already running to avoid duplicates
            exec_id = await docker_client.exec_create(
                container_id, ["sh", "-c", "pgrep -f oauth2-proxy || true"]
            )
            output = await docker_client.exec_start(exec_id)
            exec_info = await docker_client.exec_inspect(exec_id)

            if exec_info.get("ExitCode") == 0 and output.strip():
                print(f"oauth2-proxy already running in container {container_name}")
                return True

            # Start oauth2-proxy in the background
            print(
                f"Starting oauth2-proxy with upstream: {upstream_url} in container {container_name}"
            )
            cmd = [
                "sh",
                "-c",
                f"oauth2-proxy --upstream={upstream_url} > /tmp/oauth2-proxy.log 2>&1 &",
            ]
            exec_id = await docker_client.exec_create(container_id, cmd)
            await docker_client.exec_start(exec_id)
            exec_info = await docker_client.exec_inspect(exec_id)

            if exec_info.get("ExitCode", 0) == 0:
                print(
                    f"Successfully started oauth2-proxy in container {container_name}"
                )
                return True
            else:
                print(f"Failed to start oauth2-proxy in container {container_name}")
                return False

        except Exception as e:
            print(
                f"Exception while starting oauth2-proxy in container {container_name}: {str(e)}"
            )
            return False

    async def install_certificates_in_container(self, deployment_id: str):
        """Install CA certificates in a running container after deployment"""
        containers = await self.get_container(deployment_id)

        if not containers:
            return False

        container = containers[0]
        container_id = container.get("Id")
        labels = container.get("Labels", {})
        container_name = container.get("Names", [deployment_id])[0].lstrip("/")

        # Check if certificate installation is enabled via labels
        if labels.get("gitops.certs.enabled") != "true":
            return False

        # Ensure container is running
        state = container.get("State", "")
        if state != "running":
            print(
                f"Warning: Container {container_name} is not running (status: {state}), cannot install certificates"
            )
            return False

        try:
            docker_client = get_async_docker_client()

            # Install certificates: copy from custom dir, rename .pem to .crt, and update
            cert_install_script = """
if [ -d /usr/local/share/ca-certificates/custom ]; then
    cp /usr/local/share/ca-certificates/custom/*.crt /usr/local/share/ca-certificates/ 2>/dev/null || true
    cp /usr/local/share/ca-certificates/custom/*.pem /usr/local/share/ca-certificates/ 2>/dev/null || true
    for f in /usr/local/share/ca-certificates/*.pem; do
        [ -f "$f" ] && mv "$f" "${f%.pem}.crt"
    done
    update-ca-certificates 2>&1 | grep -v "WARNING" || true
    echo "CA certificates installed successfully"
else
    echo "No custom CA certificates directory found"
fi
"""
            print(f"Installing CA certificates in container {container_name}")
            cmd = ["sh", "-c", cert_install_script]
            exec_id = await docker_client.exec_create(container_id, cmd)
            output = await docker_client.exec_start(exec_id)
            exec_info = await docker_client.exec_inspect(exec_id)

            if exec_info.get("ExitCode", 0) == 0:
                print(
                    f"Successfully installed certificates in container {container_name}: {output.strip()}"
                )
                return True
            else:
                print(
                    f"Failed to install certificates in container {container_name}: {output.strip()}"
                )
                return False

        except Exception as e:
            print(
                f"Exception while installing certificates in container {container_name}: {str(e)}"
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

        containers = await self.get_container(deployment_id)
        if containers:
            await self.remove_automation(deployment_id)
        return {"status": "success", "message": message}

    async def get_tag(self, deployed_image: str):
        """Get the sha tag for a deployed image using async Docker client."""
        expected_prefix = f"{deployed_image}:sha"
        try:
            docker_client = get_async_docker_client()
            image = await docker_client.get_image(deployed_image)
            tags = image.get("RepoTags", []) or []
            for tag in tags:
                if tag.startswith(expected_prefix):
                    deployed_image_checksum_tag = tag[len(expected_prefix) :]
                    return deployed_image_checksum_tag
            return None
        except DockerError:
            return None

    async def deploy_automation(
        self,
        deployment_id: str,
        checksum: str | None = None,
        stage: str | None = None,
        relative_path: str | None = None,
        # Automation config values (for live-dev, sent by extension)
        image: str | None = None,
        expose: bool | None = None,
        port: int | None = None,
        mount_path: str | None = None,
        secret_groups: list[str] | None = None,
        automation_id: str | None = None,
        auth: bool | None = None,
        allowed_domains: list[str] | None = None,
    ):
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
        has_updates = any(
            v is not None
            for v in [
                checksum,
                stage,
                relative_path,
                image,
                expose,
                port,
                mount_path,
                secret_groups,
                automation_id,
                auth,
                allowed_domains,
            ]
        )
        if has_updates:
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

            # Store automation config values (for live-dev deployments)
            if image is not None:
                deployment_config["image"] = image
            if expose is not None:
                deployment_config["expose"] = expose
            if port is not None:
                deployment_config["port"] = port
            if mount_path is not None:
                deployment_config["mount_path"] = mount_path
            if secret_groups is not None:
                deployment_config["secret_groups"] = secret_groups
            if automation_id is not None:
                deployment_config["id"] = automation_id
            if auth is not None:
                deployment_config["auth"] = auth
            if allowed_domains is not None:
                deployment_config["allowed_domains"] = allowed_domains

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

        await self.install_certificates_in_container(deployment_id)
        await self.start_oauth2_proxy_in_container(deployment_id)

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
            await self.install_certificates_in_container(deployment_id)
            await self.start_oauth2_proxy_in_container(deployment_id)

        return {
            "message": "Deployed services successfully",
            "deployments": list(deployments.keys()),
            "result": deployment_result,
        }

    async def start_automation(self, deployment_id: str):
        """Start a container using async Docker client."""
        containers = await self.get_container(deployment_id)

        if not containers:
            raise HTTPException(
                status_code=404,
                detail=f"No container found for deployment ID: {deployment_id}",
            )

        container_id = containers[0].get("Id")
        docker_client = get_async_docker_client()
        await docker_client.start_container(container_id)

        await self.install_certificates_in_container(deployment_id)
        await self.start_oauth2_proxy_in_container(deployment_id)

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
        """Stop a container using async Docker client."""
        containers = await self.get_container(deployment_id)

        if not containers:
            raise HTTPException(
                status_code=404,
                detail=f"No container found for deployment ID: {deployment_id}",
            )

        container_id = containers[0].get("Id")
        docker_client = get_async_docker_client()
        await docker_client.stop_container(container_id)

        await self.mark_as_inactive(deployment_id)

        return {
            "status": "success",
            "message": f"Container for deployment {deployment_id} stopped successfully",
        }

    async def restart_automation(self, deployment_id: str):
        """Restart a container using async Docker client."""
        containers = await self.get_container(deployment_id)

        if not containers:
            raise HTTPException(
                status_code=404,
                detail=f"No container found for deployment ID: {deployment_id}",
            )

        container_id = containers[0].get("Id")
        docker_client = get_async_docker_client()
        await docker_client.restart_container(container_id)

        await self.install_certificates_in_container(deployment_id)
        await self.start_oauth2_proxy_in_container(deployment_id)

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

        await self.remove_automation(deployment_id)

        return {
            "status": "success",
            "message": f"Deployment {deployment_id} deactivated successfully",
        }

    async def get_automation_logs(self, deployment_id: str, lines: int = 100):
        """Get container logs using async Docker client."""
        containers = await self.get_container(deployment_id)

        if not containers:
            raise HTTPException(
                status_code=404,
                detail=f"No container found for deployment ID: {deployment_id}",
            )

        container_id = containers[0].get("Id")
        docker_client = get_async_docker_client()
        logs = await docker_client.get_container_logs(container_id, tail=lines)

        return {"status": "success", "logs": logs.split("\n")}

    async def remove_automation(self, deployment_id: str):
        """Remove a container using async Docker client."""
        containers = await self.get_container(deployment_id)

        if not containers:
            raise HTTPException(
                status_code=404,
                detail=f"No container found for deployment ID: {deployment_id}",
            )

        container_id = containers[0].get("Id")
        docker_client = get_async_docker_client()
        await docker_client.stop_container(container_id)
        await docker_client.remove_container(container_id)

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

    def get_or_create_public_client(
        self,
        client_id: str,
        redirect_uri: str,
        web_origins: list[str] | None = None,
    ):
        """
        Get or create a public Keycloak client for frontend apps.

        Args:
            client_id: The client ID for the public client
            redirect_uri: The redirect URI for this deployment
            web_origins: List of allowed CORS origins for the client

        Returns:
            dict with client_id, issuer_url, etc. or None if failed
        """
        if not self.workspace_id:
            print(
                f"Warning: Workspace {self.workspace_name} is missing an ID, skipping public client creation"
            )
            return None
        if not self.aoc_url or not self.aoc_token:
            print(
                "Warning: AOC URL or token not configured, skipping public client creation"
            )
            return None

        url = f"{self.aoc_url}/api/automation_server/workspaces/{self.workspace_id}/keycloak/public-client/"

        headers = {
            "Authorization": f"Bearer {self.aoc_token}",
            "Content-Type": "application/json",
        }
        payload = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
        }

        if web_origins:
            payload["web_origins"] = web_origins

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            if response.status_code == 200:
                result = response.json()
                print(f"Successfully got/created public client: {client_id}")
                return result
            else:
                error_detail = (
                    f"Keycloak API error: {response.status_code} - {response.text}"
                )
                print(f"Warning: Failed to get/create public client: {error_detail}")
                return None
        except Exception as e:
            print(f"Warning: Exception while getting/creating public client: {str(e)}")
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
                # For live-dev, use config values stored in bitswan.yaml (sent by extension)
                # This avoids needing filesystem access to the workspace
                stored_image = conf.get("image")
                stored_expose = conf.get("expose", False)
                stored_port = conf.get("port", 8080)
                stored_mount_path = conf.get("mount_path", "/app/")
                stored_secret_groups = conf.get("secret_groups")
                stored_id = conf.get("id")
                stored_auth = conf.get("auth", False)
                stored_allowed_domains = conf.get("allowed_domains")

                if not stored_image:
                    # Skip live-dev deployments without stored config
                    continue

                # Create a minimal AutomationConfig from stored values
                automation_config = AutomationConfig(
                    id=stored_id,
                    auth=stored_auth,
                    image=stored_image,
                    expose=stored_expose,
                    port=stored_port,
                    config_format="toml",
                    mount_path=stored_mount_path,
                    live_dev_groups=stored_secret_groups,
                    allowed_domains=stored_allowed_domains,
                )
                pipeline_conf = None
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
                if stage == "live-dev":
                    # live-dev uses its own secrets, or falls back to dev secrets
                    secret_groups = (
                        automation_config.live_dev_groups
                        or automation_config.dev_groups
                        or []
                    )
                elif stage == "dev" and automation_config.dev_groups:
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

            # Always pass Keycloak URL for JWT verification
            # KEYCLOAK_URL format: https://keycloak.example.com/realms/realm-name
            keycloak_url = os.environ.get("KEYCLOAK_URL", "")
            if keycloak_url:
                entry["environment"]["KEYCLOAK_URL"] = (
                    keycloak_url.rsplit("/realms/", 1)[0]
                    if "/realms/" in keycloak_url
                    else keycloak_url
                )
                entry["environment"]["KEYCLOAK_REALM"] = (
                    keycloak_url.rsplit("/realms/", 1)[-1]
                    if "/realms/" in keycloak_url
                    else ""
                )
                entry["environment"]["KEYCLOAK_ISSUER_URL"] = keycloak_url

            # Handle Keycloak public client for frontend apps
            # Uses auth + id approach: when auth=true, id is used as Keycloak client_id
            if automation_config.auth and automation_config.id:
                # Build the redirect URI for this deployment
                automation_url = f"https://{self.workspace_name}-{deployment_id}.{self.gitops_domain}"
                redirect_uri = f"{automation_url}/*"

                # Build web_origins for CORS: automation URL + any allowed_domains from config
                web_origins = [automation_url]
                if automation_config.allowed_domains:
                    web_origins.extend(automation_config.allowed_domains)

                # Get or create the public client via AOC
                client_result = self.get_or_create_public_client(
                    client_id=automation_config.id,
                    redirect_uri=redirect_uri,
                    web_origins=web_origins,
                )

                if client_result:
                    # Inject Keycloak client ID and override URL if available from client result
                    entry["environment"]["KEYCLOAK_CLIENT_ID"] = client_result.get(
                        "client_id", automation_config.id
                    )
                    if client_result.get("issuer_url"):
                        issuer_url = client_result.get("issuer_url")
                        entry["environment"]["KEYCLOAK_URL"] = (
                            issuer_url.rsplit("/realms/", 1)[0]
                            if "/realms/" in issuer_url
                            else issuer_url
                        )
                        entry["environment"]["KEYCLOAK_REALM"] = (
                            issuer_url.rsplit("/realms/", 1)[-1]
                            if "/realms/" in issuer_url
                            else ""
                        )
                        entry["environment"]["KEYCLOAK_ISSUER_URL"] = issuer_url
                    print(
                        f"Injected Keycloak config for client {automation_config.id} into deployment {deployment_id}"
                    )
                else:
                    print(
                        f"Warning: Failed to get/create public client {automation_config.id} for deployment {deployment_id}"
                    )

            if "volumes" not in entry:
                entry["volumes"] = []

            # Mount CA certificates if configured
            if self.certs_dir_host:
                entry["volumes"].append(
                    f"{self.certs_dir_host}:/usr/local/share/ca-certificates/custom:ro"
                )
                entry["environment"]["UPDATE_CA_CERTIFICATES"] = "true"
                entry["labels"]["gitops.certs.enabled"] = "true"

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
