import asyncio
import json
import os
import secrets
import time
from typing import Any, Dict, List, Optional
from configparser import ConfigParser

import aiohttp
from aiohttp import ClientConnectorError
from fastapi import HTTPException

from app.utils import add_route_to_caddy, read_bitswan_yaml, parse_pipeline_conf, parse_automation_toml


class ContainerInfo:
    """Simple container info object to maintain compatibility with existing code."""

    def __init__(self, container_data: Dict[str, Any]):
        self.id = container_data.get("Id", "")
        self.name = container_data.get("Names", [""])[0].lstrip("/")
        self.status = container_data.get("Status", "")
        self.labels = container_data.get("Labels", {})


class DockerAPIClient:
    """Async Docker API client using aiohttp with Unix socket."""

    def __init__(self):
        # Unix socket path for Docker daemon - this is where all connections go
        self.socket_path = "/var/run/docker.sock"
        # base_url is required by aiohttp.ClientSession but is not used for actual connections
        # When using UnixConnector, all HTTP requests are routed through the Unix socket
        # This is just a placeholder for URL parsing
        self.base_url = "http://localhost"
        self.connector = None
        self._session = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session with Unix socket connector."""
        if self._session is None or self._session.closed:
            if self.connector is None or self.connector.closed:
                self.connector = aiohttp.UnixConnector(path=self.socket_path)
            # base_url is required by ClientSession but not used with UnixConnector
            # All requests go through the Unix socket at self.socket_path
            self._session = aiohttp.ClientSession(
                connector=self.connector, base_url=self.base_url
            )
        return self._session

    async def _request(
        self,
        method: str,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Make async HTTP request to Docker API."""
        session = await self._get_session()
        try:
            async with session.request(
                method, path, json=json_data, params=params
            ) as response:
                if response.status == 404:
                    raise HTTPException(status_code=404, detail="Container not found")
                if response.status >= 400:
                    error_text = await response.text()
                    raise HTTPException(
                        status_code=response.status,
                        detail=f"Docker API error: {error_text}",
                    )
                content_type = response.headers.get("Content-Type", "")
                if "application/json" in content_type:
                    result = await response.json()
                    await response.release()
                    return result
                # For non-JSON responses, consume and release
                await response.read()
                await response.release()
                return {"status": response.status}
        except ClientConnectorError as e:
            print(f"Failed to connect to Docker: {e}")
            raise HTTPException(
                status_code=500, detail=f"Failed to connect to Docker: {e}"
            )

    async def list_containers(
        self, filters: Optional[Dict[str, List[str]]] = None, all: bool = False
    ) -> List[Dict[str, Any]]:
        """List containers."""
        params = {"all": "true" if all else "false"}
        if filters:
            params["filters"] = json.dumps(filters)
        result = await self._request("GET", "/containers/json", params=params)
        return result if isinstance(result, list) else []

    async def get_container(self, container_name: str) -> Dict[str, Any]:
        """Get container information."""
        # First, get container ID from name
        containers = await self.list_containers(all=True)
        for container in containers:
            names = container.get("Names", [])
            if any(name.lstrip("/") == container_name for name in names):
                container_id = container["Id"]
                return await self._request("GET", f"/containers/{container_id}/json")
        raise HTTPException(status_code=404, detail="Container not found")

    async def get_container_logs(self, container_id: str, tail: int = 50) -> str:
        """Get container logs."""
        params = {"tail": str(tail), "stdout": "true", "stderr": "true"}
        session = await self._get_session()
        try:
            async with session.get(
                f"/containers/{container_id}/logs", params=params
            ) as response:
                if response.status >= 400:
                    error_text = await response.text()
                    await response.release()
                    return f"Error getting logs: {response.status} - {error_text}"
                logs = await response.text()
                await response.release()
                return logs
        except Exception as e:
            return f"Error reading logs: {str(e)}"

    async def create_container(
        self,
        image: str,
        command: List[str],
        name: str,
        network: str,
        labels: Dict[str, str],
        volumes: Optional[List[str]] = None,
        working_dir: Optional[str] = None,
        env_file: Optional[List[str]] = None,
        network_aliases: Optional[List[str]] = None,
        env: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Create a container."""
        config = {
            "Image": image,
            "Cmd": command,
            "Labels": labels,
            "HostConfig": {
                "RestartPolicy": {"Name": "unless-stopped"},
            },
        }

        # Use NetworkingConfig to connect to network with aliases
        if network_aliases:
            config["NetworkingConfig"] = {
                "EndpointsConfig": {network: {"Aliases": network_aliases}}
            }
        else:
            # Fall back to NetworkMode if no aliases
            config["HostConfig"]["NetworkMode"] = network

        if working_dir:
            config["WorkingDir"] = working_dir

        if volumes:
            config["HostConfig"]["Binds"] = volumes

        # Initialize env_vars list
        env_vars = []

        # Add additional environment variables first (defaults)
        if env:
            env_vars.extend(env)
            print(f"DEBUG: Added {len(env)} additional env vars to container config")

        # Read env files and merge into environment variables (secrets override defaults)
        if env_file:
            print(f"DEBUG: Reading {len(env_file)} env file(s)")
            for env_file_path in env_file:
                print(f"DEBUG: Reading env file: {env_file_path}")
                if os.path.exists(env_file_path):
                    with open(env_file_path, "r") as f:
                        file_vars = []
                        for line in f:
                            line = line.strip()
                            if line and not line.startswith("#") and "=" in line:
                                file_vars.append(line)
                                env_vars.append(line)
                        print(
                            f"DEBUG: Read {len(file_vars)} env vars from {env_file_path}"
                        )
                else:
                    print(f"WARNING: Env file does not exist: {env_file_path}")

        if env_vars:
            config["Env"] = env_vars
            print(f"DEBUG: Added {len(env_vars)} total env vars to container config")

        params = {"name": name}
        return await self._request(
            "POST", "/containers/create", json_data=config, params=params
        )

    async def start_container(self, container_id: str) -> None:
        """Start a container."""
        await self._request("POST", f"/containers/{container_id}/start")

    async def stop_container(self, container_id: str) -> None:
        """Stop a container."""
        await self._request("POST", f"/containers/{container_id}/stop")

    async def remove_container(self, container_id: str) -> None:
        """Remove a container."""
        await self._request("DELETE", f"/containers/{container_id}")

    async def exec_create(
        self, container_id: str, cmd: List[str], user: str = "root"
    ) -> Dict[str, Any]:
        """Create an exec instance."""
        config = {
            "AttachStdout": True,
            "AttachStderr": True,
            "Cmd": cmd,
            "User": user,
        }
        return await self._request(
            "POST", f"/containers/{container_id}/exec", json_data=config
        )

    async def exec_start(self, exec_id: str) -> tuple[int, bytes]:
        """Start an exec instance and get output."""
        config = {"Detach": False, "Tty": False}
        session = await self._get_session()
        # Start the exec (using relative path - connection goes through Unix socket)
        async with session.post(
            f"/exec/{exec_id}/start",
            json=config,
        ) as response:
            if response.status >= 400:
                error_text = await response.text()
                raise HTTPException(
                    status_code=response.status,
                    detail=f"Docker exec error: {error_text}",
                )
            # Read the output stream
            output = b""
            async for chunk in response.content.iter_chunked(8192):
                output += chunk

        # Get exec instance info to check exit code
        async with session.get(f"/exec/{exec_id}/json") as info_response:
            if info_response.status == 200:
                exec_info = await info_response.json()
                exit_code = exec_info.get("ExitCode", -1)
            else:
                exit_code = -1
            # Ensure response is fully consumed and closed
            await info_response.release()
        return (exit_code, output)

    async def close(self):
        """Close the session and connector."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        if self.connector and not self.connector.closed:
            await self.connector.close()
            self.connector = None


class JupyterService:
    def __init__(self):
        self.token_auth_enabled = (
            os.environ.get("JUPYTER_SERVER_ENABLE_TOKEN_AUTH", "true").lower()
            != "false"
        )
        self.docker_client = DockerAPIClient()
        # Initialize paths - use host paths for mounting volumes into containers
        self.bs_home_host = os.environ.get(
            "BITSWAN_GITOPS_DIR_HOST", "/home/root/.config/bitswan/local-gitops/"
        )
        # Paths inside gitops container for reading config files
        self.bs_home = os.environ.get("BITSWAN_GITOPS_DIR", "/mnt/repo/pipeline")
        self.gitops_dir = os.path.join(self.bs_home, "gitops")
        # Secrets directory - use container path like automation_service
        # Since we're reading from the container's filesystem, use container path
        self.secrets_dir = os.path.join(self.bs_home, "secrets")
        self.workspace_name = os.environ.get(
            "BITSWAN_WORKSPACE_NAME", "workspace-local"
        )

    async def start_jupyter_server(
        self,
        automation_name: str,
        pre_image: str,
        session_id: str,
        server_token: str = None,
        relative_path: str = "",
        pipelines_conf_content: str = "",
    ):
        container_name = f"{automation_name}-{session_id}-jupyter-server"

        await self.check_container_exists(container_name)

        token = ""
        if self.token_auth_enabled:
            if server_token:
                token = server_token
            else:
                token = self.generate_jupyter_server_token()

        container_id = await self.create_jupyter_server(
            pre_image=pre_image,
            container_name=container_name,
            token=token,
            session_id=session_id,
            automation_name=automation_name,
            relative_path=relative_path,
            pipelines_conf_content=pipelines_conf_content,
        )

        await self.docker_client.start_container(container_id)
        await self.wait_for_container_port(container_name, 80)

        (exit_code, output) = await self.exec_in_container(
            container_id,
            [
                "python",
                "-m",
                "ipykernel",
                "install",
                "--user",
                "--name",
                f"{automation_name}_kernel",
                "--display-name",
                f"{automation_name} kernel",
            ],
            user="root",
        )

        if exit_code != 0:
            raise HTTPException(
                status_code=500, detail=f"Error installing ipykernel: {output}"
            )

        jupyter_server_host = self.generate_jupyter_server_caddy_url(
            container_name, full=False
        )

        success = await asyncio.to_thread(
            add_route_to_caddy,
            jupyter_server_host,
            container_name,
            f"{container_name}:80",
        )
        if not success:
            raise HTTPException(
                status_code=500,
                detail=f"Error adding route to Caddy: {jupyter_server_host}",
            )

        # Give Caddy a moment to activate the route before returning
        # This helps prevent connection refused errors when VS Code tries to connect immediately
        await asyncio.sleep(1)

        jupyter_server_full_url = self.generate_jupyter_server_caddy_url(
            container_name, full=True
        )

        return {
            "status": "success",
            "message": "Jupyter server started successfully",
            "server_info": {
                "pre": pre_image,
                "url": jupyter_server_full_url,
                "token": token,
            },
        }

    def _get_automation_config(self, automation_name: str) -> Dict[str, Any]:
        """Get automation configuration from bitswan.yaml."""
        bs_yaml = read_bitswan_yaml(self.gitops_dir)
        if not bs_yaml or "deployments" not in bs_yaml:
            return {}

        deployment_config = bs_yaml.get("deployments", {}).get(automation_name, {})
        return deployment_config or {}

    def _get_secret_groups(
        self, pipeline_conf: Optional[ConfigParser], automation_name: str
    ) -> List[str]:
        """Get secret groups for an automation from pipeline_conf."""
        if not pipeline_conf:
            return []

        # Get stage from automation config
        config = self._get_automation_config(automation_name)
        stage = config.get("stage", "production")
        if stage == "":
            stage = "production"

        # Check for stage-specific secret groups first, then fall back to groups
        stage_groups_key = f"{stage}_groups"
        secret_groups_str = pipeline_conf.get("secrets", stage_groups_key, fallback="")
        if not secret_groups_str:
            # Fall back to generic groups if stage-specific groups not set
            secret_groups_str = pipeline_conf.get("secrets", "groups", fallback="")
        secret_groups = secret_groups_str.split(" ") if secret_groups_str else []

        return secret_groups

    def _get_secret_env_files(
        self, pipeline_conf: Optional[ConfigParser], automation_name: str
    ) -> List[str]:
        """Get list of secret env file paths for an automation (container paths)."""
        secret_groups = self._get_secret_groups(pipeline_conf, automation_name)
        env_files = []

        if not secret_groups:
            print(f"DEBUG: No secret groups found for {automation_name}")
            return env_files

        print(f"DEBUG: Secret groups for {automation_name}: {secret_groups}")
        print(f"DEBUG: Secrets directory: {self.secrets_dir}")

        # Use container path like automation_service - we're reading from container filesystem
        if not os.path.exists(self.secrets_dir):
            print(f"WARNING: Secrets directory does not exist: {self.secrets_dir}")
            return env_files

        for secret_group in secret_groups:
            # Skip empty secret groups
            if not secret_group:
                continue
            secret_env_file = os.path.join(self.secrets_dir, secret_group)
            print(f"DEBUG: Checking secret file: {secret_env_file}")
            if os.path.exists(secret_env_file):
                env_files.append(secret_env_file)
                print(f"DEBUG: Added secret env file: {secret_env_file}")
            else:
                print(f"WARNING: Secret file does not exist: {secret_env_file}")

        print(f"DEBUG: Total secret env files found: {len(env_files)}")
        return env_files

    async def create_jupyter_server(
        self,
        pre_image: str,
        container_name: str,
        token: str,
        session_id: str,
        automation_name: str,
        relative_path: str = "",
        pipelines_conf_content: str = "",
        deployment_id: str = "",
    ) -> str:
        """Create a Jupyter server container and return its ID."""
        try:
            allowed_origins = os.environ.get("JUPYTER_SERVER_ALLOWED_ORIGINS", "")
            disable_xsrf_check = (
                os.environ.get("JUPYTER_SERVER_DISABLE_XSRF_CHECK", "false").lower()
                == "true"
            )

            # Parse pipelines.conf content if provided
            pipeline_conf = (
                parse_pipeline_conf(pipelines_conf_content)
                if pipelines_conf_content
                else None
            )

            # Mount workspace directory
            workspace_host_path = os.path.join(self.bs_home_host, "workspace")
            volumes = [f"{workspace_host_path}:/workspace:z"]

            # Set working directory
            # If relative_path is provided, use it; otherwise use /workspace
            if relative_path:
                working_dir = f"/workspace/{relative_path.lstrip('/')}"
            else:
                working_dir = "/workspace"

            # Get secret env files
            secret_env_files = self._get_secret_env_files(
                pipeline_conf, automation_name
            )

            # Get the Caddy domain for network alias
            jupyter_server_host = self.generate_jupyter_server_caddy_url(
                container_name, full=False
            )

            # Build labels
            labels = {
                "bitswan.type": "jupyter_kernel",
                "bitswan.automation_name": automation_name,
            }
            if deployment_id:
                labels["bitswan.deployment_id"] = deployment_id
            if token:
                labels["bitswan.token"] = token

            # Set PYTHONPATH to include bitswan_lib
            # If PYTHONPATH is set in secrets, it will override this value
            env_vars = ["PYTHONPATH=/workspace/bitswan_lib"]

            container_data = await self.docker_client.create_container(
                image=pre_image,
                command=[
                    "jupyter",
                    "notebook",
                    "--ip=0.0.0.0",
                    "--port=80",
                    "--no-browser",
                    "--allow-root",
                    f"--NotebookApp.token={token}",
                    f"--NotebookApp.disable_check_xsrf={disable_xsrf_check}",
                    f"--NotebookApp.allow_origin={allowed_origins}",
                    "--NotebookApp.shutdown_no_activity_timeout=0",  # Don't auto-shutdown
                    "--MappingKernelManager.cull_idle_timeout=0",  # Don't cull idle kernels
                    "--MappingKernelManager.cull_interval=0",  # Disable kernel culling
                ],
                name=container_name,
                network="bitswan_network",
                labels=labels,
                volumes=volumes,
                working_dir=working_dir,
                env_file=secret_env_files,
                env=env_vars,
                network_aliases=[jupyter_server_host],
            )

            return container_data["Id"]

        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error creating jupyter server: {str(e)}"
            )

    async def check_container_exists(self, container_name: str):
        """Check if container exists and remove it if it does."""
        try:
            containers = await self.docker_client.list_containers(all=True)
            for container in containers:
                names = container.get("Names", [])
                if any(name.lstrip("/") == container_name for name in names):
                    container_id = container["Id"]
                    if container.get("State") == "running":
                        await self.docker_client.stop_container(container_id)
                    await self.docker_client.remove_container(container_id)
        except HTTPException as e:
            if e.status_code != 404:
                raise

    async def get_jupyter_server_containers(
        self, all: bool = False
    ) -> List[ContainerInfo]:
        """Get all Jupyter server containers."""
        containers = await self.docker_client.list_containers(
            filters={"label": ["bitswan.type=jupyter_server"]}, all=all
        )
        return [ContainerInfo(container) for container in containers]

    async def get_kernel_container(
        self, deployment_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get kernel container for a deployment_id."""
        containers = await self.docker_client.list_containers(
            filters={
                "label": [
                    f"bitswan.deployment_id={deployment_id}",
                    "bitswan.type=jupyter_kernel",
                ]
            },
            all=True,
        )
        if containers:
            container_name = (
                containers[0]["Names"][0].lstrip("/")
                if containers[0].get("Names")
                else ""
            )
            return await self.docker_client.get_container(container_name)
        return None

    async def get_kernel_details(self, deployment_id: str) -> Dict[str, Any]:
        """Get kernel status and connection details for a deployment_id."""
        containers = await self.docker_client.list_containers(
            filters={
                "label": [
                    f"bitswan.deployment_id={deployment_id}",
                    "bitswan.type=jupyter_kernel",
                ]
            },
            all=True,
        )
        if not containers:
            return {"running": False}

        container = containers[0]
        container_state = container.get("State", "unknown")
        container_name = (
            container["Names"][0].lstrip("/") if container.get("Names") else ""
        )
        labels = container.get("Labels", {})

        if container_state != "running":
            return {"running": False, "status": container_state}

        # Get connection details
        token = labels.get("bitswan.token", "")
        jupyter_server_full_url = self.generate_jupyter_server_caddy_url(
            container_name, full=True
        )

        return {
            "running": True,
            "status": container_state,
            "container_name": container_name,
            "connection": {
                "url": jupyter_server_full_url,
                "token": token,
            },
        }

    async def list_kernels_with_details(self) -> List[Dict[str, Any]]:
        """List all kernel containers with connection details."""
        containers = await self.docker_client.list_containers(
            filters={"label": ["bitswan.type=jupyter_kernel"]}, all=True
        )
        result = []
        for container in containers:
            container_name = (
                container["Names"][0].lstrip("/") if container.get("Names") else ""
            )
            container_state = container.get("State", "unknown")
            labels = container.get("Labels", {})
            deployment_id = labels.get("bitswan.deployment_id", "")

            if container_state == "running":
                token = labels.get("bitswan.token", "")
                jupyter_server_full_url = self.generate_jupyter_server_caddy_url(
                    container_name, full=True
                )
                result.append(
                    {
                        "deployment_id": deployment_id,
                        "running": True,
                        "status": container_state,
                        "container_name": container_name,
                        "connection": {
                            "url": jupyter_server_full_url,
                            "token": token,
                        },
                    }
                )
            else:
                result.append(
                    {
                        "deployment_id": deployment_id,
                        "running": False,
                        "status": container_state,
                        "container_name": container_name,
                    }
                )
        return result

    async def stop_kernel(self, deployment_id: str) -> Dict[str, Any]:
        """Stop and remove kernel container for a deployment_id."""
        containers = await self.docker_client.list_containers(
            filters={
                "label": [
                    f"bitswan.deployment_id={deployment_id}",
                    "bitswan.type=jupyter_kernel",
                ]
            },
            all=True,
        )
        if not containers:
            raise HTTPException(
                status_code=404,
                detail=f"Kernel not found for deployment_id: {deployment_id}",
            )

        container = containers[0]
        container_id = container["Id"]
        container_state = container.get("State", "unknown")

        # Stop container
        if container_state == "running":
            await self.docker_client.stop_container(container_id)

        # Note: We'd need a remove_route_from_caddy function, but for now just remove the container
        # The route will be cleaned up when the container is removed

        # Remove container
        await self.docker_client.remove_container(container_id)

        return {
            "status": "success",
            "message": f"Kernel stopped for deployment_id: {deployment_id}",
        }

    async def start_kernel(
        self,
        deployment_id: str,
        relative_path: str = "",
        pipelines_conf_content: str = "",
        automation_toml_content: str = "",
    ) -> Dict[str, Any]:
        """Start a Jupyter kernel for a deployment_id."""
        # Parse config to get pre image - try automation.toml first, then pipelines.conf
        pre_image = None
        pipeline_conf = None

        # Try automation.toml first (highest priority)
        if automation_toml_content:
            automation_config = parse_automation_toml(automation_toml_content)
            if automation_config:
                pre_image = automation_config.image

        # Fall back to pipelines.conf
        if not pre_image and pipelines_conf_content:
            pipeline_conf = parse_pipeline_conf(pipelines_conf_content)
            if pipeline_conf:
                pre_image = pipeline_conf.get("deployment", "pre", fallback=None)

        if not pre_image:
            raise HTTPException(
                status_code=400,
                detail="Config file required: automation.toml with 'image' or pipelines.conf with 'pre'"
            )

        # Parse pipelines.conf for secrets (still needed for secret groups)
        if not pipeline_conf and pipelines_conf_content:
            pipeline_conf = parse_pipeline_conf(pipelines_conf_content)

        container_name = f"{self.workspace_name}__{deployment_id}-jupyter-kernel"

        # Check if kernel already exists and stop it
        existing_container = await self.get_kernel_container(deployment_id)
        if existing_container:
            await self.stop_kernel(deployment_id)

        # Generate token
        token = ""
        if self.token_auth_enabled:
            token = self.generate_jupyter_server_token()

        # Create and start container
        container_id = await self.create_jupyter_server(
            pre_image=pre_image,
            container_name=container_name,
            token=token,
            session_id="",  # Not used anymore
            automation_name=deployment_id,
            relative_path=relative_path,
            pipelines_conf_content=pipelines_conf_content,
            deployment_id=deployment_id,
        )

        await self.docker_client.start_container(container_id)
        await self.wait_for_container_port(container_name, 80)

        # Install ipykernel
        (exit_code, output) = await self.exec_in_container(
            container_id,
            [
                "python",
                "-m",
                "ipykernel",
                "install",
                "--user",
                "--name",
                f"{deployment_id}_kernel",
                "--display-name",
                f"{deployment_id} kernel",
            ],
            user="root",
        )

        if exit_code != 0:
            raise HTTPException(
                status_code=500, detail=f"Error installing ipykernel: {output}"
            )

        # Add Caddy route
        jupyter_server_host = self.generate_jupyter_server_caddy_url(
            container_name, full=False
        )
        success = await asyncio.to_thread(
            add_route_to_caddy,
            jupyter_server_host,
            container_name,
            f"{container_name}:80",
        )
        if not success:
            raise HTTPException(
                status_code=500,
                detail=f"Error adding route to Caddy: {jupyter_server_host}",
            )

        await asyncio.sleep(1)

        jupyter_server_full_url = self.generate_jupyter_server_caddy_url(
            container_name, full=True
        )

        # Get secret groups for the response
        secret_groups = self._get_secret_groups(pipeline_conf, deployment_id)
        secret_groups_str = ", ".join(secret_groups) if secret_groups else "none"

        return {
            "status": "success",
            "message": "Kernel started successfully",
            "connection": {
                "url": jupyter_server_full_url,
                "token": token,
            },
            "config": {
                "pipelines_conf_content": pipelines_conf_content,
                "secret_groups": secret_groups_str,
                "pre_image": pre_image,
            },
        }

    async def exec_in_container(
        self, container_id: str, cmd: List[str], user: str = "root"
    ) -> tuple[int, bytes]:
        """Execute a command in a container."""
        exec_data = await self.docker_client.exec_create(container_id, cmd, user)
        exec_id = exec_data["Id"]
        return await self.docker_client.exec_start(exec_id)

    def generate_jupyter_server_caddy_url(self, server_name, full=False):
        gitops_domain = os.environ.get("BITSWAN_GITOPS_DOMAIN")
        url = f"{server_name}.{gitops_domain}"

        use_https = os.environ.get("BITSWAN_USE_HTTPS", "false").lower() == "true"

        if use_https:
            return f"https://{url}" if full else url
        else:
            return f"http://{url}" if full else url

    async def wait_for_container_port(
        self, container_name: str, port: int, timeout: int = 30
    ):
        """Wait for container port to become available, checking container status first."""
        start_time = time.time()
        container_id = None

        # First, verify container exists and is running
        try:
            container_info = await self.docker_client.get_container(container_name)
            container_id = container_info.get("Id")
            container_state = container_info.get("State", {})
            container_status = container_state.get("Status", "unknown")

            if container_status != "running":
                # Get logs to help debug
                logs = await self.docker_client.get_container_logs(
                    container_id, tail=20
                )
                raise HTTPException(
                    status_code=500,
                    detail=f"Container '{container_name}' is not running (status: {container_status}). Logs: {logs[-500:]}",
                )
        except HTTPException:
            raise
        except Exception as e:
            # If we can't get container info, continue with port check
            print(f"Warning: Could not get container info: {e}")

        # Now check if port is accessible
        while time.time() - start_time < timeout:
            try:
                # Use asyncio.open_connection for async socket connection
                try:
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(container_name, port),
                        timeout=2.0,
                    )
                    writer.close()
                    await writer.wait_closed()
                    return True
                except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
                    # Check container status periodically
                    elapsed = time.time() - start_time
                    if int(elapsed) % 5 == 0:  # Every 5 seconds
                        try:
                            if container_id:
                                container_info = await self.docker_client.get_container(
                                    container_name
                                )
                                container_state = container_info.get("State", {})
                                container_status = container_state.get(
                                    "Status", "unknown"
                                )
                                if container_status != "running":
                                    logs = await self.docker_client.get_container_logs(
                                        container_id, tail=20
                                    )
                                    raise HTTPException(
                                        status_code=500,
                                        detail=f"Container '{container_name}' stopped (status: {container_status}). Logs: {logs[-500:]}",
                                    )
                        except HTTPException:
                            raise
                        except Exception:
                            pass  # Ignore errors checking status
                    await asyncio.sleep(1)
            except HTTPException:
                raise
            except Exception:
                await asyncio.sleep(1)

        # Timeout - get logs for debugging
        try:
            if not container_id:
                container_info = await self.docker_client.get_container(container_name)
                container_id = container_info.get("Id")
            logs = await self.docker_client.get_container_logs(container_id, tail=30)
            raise HTTPException(
                status_code=500,
                detail=f"Jupyter server in container '{container_name}' did not become ready within {timeout} seconds. Container logs: {logs[-1000:]}",
            )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Jupyter server in container '{container_name}' did not become ready within {timeout} seconds. Could not get logs: {str(e)}",
            )

    def generate_jupyter_server_token(self, length: int = 32):
        return secrets.token_hex(length // 2)
