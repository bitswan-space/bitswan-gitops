from datetime import datetime
import os
import docker
from app.models import DeployedPRE
from app.utils import calculate_uptime, read_bitswan_yaml


class AutomationService:

    def get_automations(self):
        bs_home = os.environ.get("BITSWAN_GITOPS_DIR", "/mnt/repo/pipeline")
        gitops_dir = os.path.join(bs_home, "gitops")
        bs_yaml = read_bitswan_yaml(gitops_dir)

        if not bs_yaml:
            return []

        pres = {
            deployment_id: DeployedPRE(
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

        client = docker.from_env()
        info = client.info()
        containers: list[docker.models.containers.Container] = client.containers.list(
            filters={
                "label": [
                    "space.bitswan.pipeline.protocol-version",
                    "gitops.deployment_id",
                ]
            }
        )

        # updated pres with active containers
        for container in containers:
            deployment_id = container.labels["gitops.deployment_id"]
            if deployment_id in pres:
                pres[deployment_id] = DeployedPRE(
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

    def create_automation(self, deployment_id: str):
        pass

    def delete_automation(self, deployment_id: str):
        pass

    def deploy_automation(self, deployment_id: str):
        pass

    def start_automation(self, deployment_id: str):
        pass

    def stop_automation(self, deployment_id: str):
        pass

    def restart_automation(self, deployment_id: str):
        pass

    def activate_automation(self, deployment_id: str):
        pass

    def deactivate_automation(self, deployment_id: str):
        pass

    def get_automation_logs(self, deployment_id: str, lines: int = 100):
        pass