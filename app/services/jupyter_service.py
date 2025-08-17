import os
import secrets
import socket
import time

import docker
from fastapi import HTTPException

from app.utils import add_route_to_caddy


class JupyterService:
    def __init__(self):
        self.token_auth_enabled = (
            os.environ.get("JUPYTER_SERVER_ENABLE_TOKEN_AUTH", "true").lower()
            != "false"
        )

    def start_jupyter_server(
        self,
        automation_name: str,
        pre_image: str,
        session_id: str,
        server_token: str = None,
    ):

        container_name = f"{automation_name}-{session_id}-jupyter-server"

        self.check_container_exists(container_name)

        token = ""
        if self.token_auth_enabled:
            if server_token:
                token = server_token
            else:
                token = self.generate_jupyter_server_token()

        jupyter_server_container = self.create_jupyter_server(
            pre_image=pre_image,
            container_name=container_name,
            token=token,
            session_id=session_id,
        )

        jupyter_server_container.start()
        self.wait_for_container_port(container_name, 8888)

        (exit_code, output) = jupyter_server_container.exec_run(
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
            raise HTTPException(status_code=500, detail="Error installing ipykernel")

        container_name = jupyter_server_container.name

        jupyter_server_host = self.generate_jupyter_server_caddy_url(
            container_name, full=False
        )

        success = add_route_to_caddy(
            jupyter_server_host,
            container_name,
            f"{container_name}:8888",
        )
        if not success:
            raise HTTPException(status_code=500, detail="Error adding route to Caddy")

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

    def create_jupyter_server(
        self, pre_image: str, container_name: str, token: str, session_id: str
    ) -> docker.models.containers.Container:
        try:
            docker_client = docker.from_env()

            allowed_origins = os.environ.get("JUPYTER_SERVER_ALLOWED_ORIGINS", "*")
            disable_xsrf_check = (
                os.environ.get("JUPYTER_SERVER_DISABLE_XSRF_CHECK", "false").lower()
                == "true"
            )

            return docker_client.containers.create(
                image=pre_image,
                command=[
                    "jupyter",
                    "notebook",
                    "--ip=0.0.0.0",
                    "--port=8888",
                    "--no-browser",
                    f"--NotebookApp.token={token}",
                    f"--NotebookApp.disable_check_xsrf={disable_xsrf_check}",
                    f"--NotebookApp.allow_origin={allowed_origins}",
                ],
                name=container_name,
                network="bitswan_network",
                labels={
                    "bitswan.type": "jupyter_server",
                    "bitswan.automation_name": container_name,
                    "bitswan.session_id": session_id,
                },
            )

        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error creating jupyter server: {e}"
            )

    def check_container_exists(self, container_name: str):
        try:
            docker_client = docker.from_env()
            existing = docker_client.containers.get(container_name)
            if existing.status == "running":
                existing.stop()
            existing.remove()
        except docker.errors.NotFound:
            pass

    def get_jupyter_server_containers(self, all: bool = False):
        docker_client = docker.from_env()
        containers = docker_client.containers.list(
            filters={"label": "bitswan.type=jupyter_server"},
            all=all,
        )
        return containers

    def teardown_jupyter_server_container(self, server_name: str):
        docker_client = docker.from_env()
        container = docker_client.containers.get(server_name)
        container.stop()
        container.remove()

    def generate_jupyter_server_caddy_url(self, server_name, full=False):
        gitops_domain = os.environ.get("BITSWAN_GITOPS_DOMAIN", "gitops.bitswan.space")
        url = f"{server_name}.{gitops_domain}"

        use_https = os.environ.get("BITSWAN_USE_HTTPS", "false").lower() == "true"

        if use_https:
            return f"https://{url}" if full else url
        else:
            return f"http://{url}" if full else url

    def wait_for_container_port(
        self, container_name: str, port: int, timeout: int = 30
    ):
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:

                with socket.create_connection((container_name, port), timeout=2):
                    return True
            except (socket.timeout, ConnectionRefusedError, OSError):
                time.sleep(1)
        raise HTTPException(
            status_code=500,
            detail=f"Jupyter server in container '{container_name}' did not become ready in time",
        )

    def generate_jupyter_server_token(self, length: int = 32):
        return secrets.token_hex(length // 2)
