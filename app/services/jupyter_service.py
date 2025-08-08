import os
import socket
import time

import docker
from fastapi import HTTPException

from app.utils import add_route_to_caddy


class JupyterService:
    def __init__(self):
        pass

    def start_jupyter_server(self, automation_name: str, pre_image: str):

        container_name = f"{automation_name}-jupyter-server"

        self.check_container_exists(container_name)

        jupyter_server_container = self.create_jupyter_server(
            pre_image=pre_image,
            container_name=container_name,
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
            print("error starting kernel: ", output)
            raise HTTPException(status_code=500, detail="Error installing ipykernel")

        container_name = jupyter_server_container.name

        jupyter_server_url = self.generate_jupyter_server_caddy_url(
            container_name, full=False
        )

        success = add_route_to_caddy(
            jupyter_server_url,
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
                "token": "",
            },
        }

    def create_jupyter_server(
        self, pre_image: str, container_name: str
    ) -> docker.models.containers.Container:
        try:
            docker_client = docker.from_env()

            return docker_client.containers.create(
                image=pre_image,
                command=[
                    "jupyter",
                    "notebook",
                    "--ip=0.0.0.0",
                    "--port=8888",
                    "--no-browser",
                    "--NotebookApp.token=",
                    "--NotebookApp.disable_check_xsrf=True",
                    "--NotebookApp.allow_origin=*",
                ],
                name=container_name,
                # ports={f"{host_port}/tcp": host_port},
                network="bitswan_network",
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
