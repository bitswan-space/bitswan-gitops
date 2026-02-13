"""
Kafka infrastructure service management.

Ported from bitswan-automation-server/internal/services/kafka.go.
"""

import base64
import logging
import os
import secrets as secrets_module

from app.services.infra_service import (
    InfraService,
    generate_password,
    run_docker_command,
)

logger = logging.getLogger(__name__)

# Entrypoint script that builds the JAAS config from KAFKA_ADMIN_PASSWORD
# env var, then execs the original confluent entrypoint.
# NOTE: $$ is docker-compose escaping — becomes $ in the actual shell command.
KAFKA_ENTRYPOINT_SCRIPT = """\
cat > /etc/kafka/kafka_server_jaas.conf <<JAASEOF
KafkaServer {
   org.apache.kafka.common.security.plain.PlainLoginModule required
   username="admin"
   password="$$KAFKA_ADMIN_PASSWORD"
   user_admin="$$KAFKA_ADMIN_PASSWORD";
};

Client {
   org.apache.kafka.common.security.plain.PlainLoginModule required
   username="admin"
   password="$$KAFKA_ADMIN_PASSWORD"
   user_admin="$$KAFKA_ADMIN_PASSWORD";
};
JAASEOF
exec /etc/confluent/docker/run
"""


def generate_cluster_id() -> str:
    """Generate a random Kafka cluster ID (URL-safe base64-encoded 16 random bytes)."""
    raw = secrets_module.token_bytes(16)
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


class KafkaService(InfraService):
    """Manages Kafka service deployment (Kafka broker + Kafka UI)."""

    DEFAULT_KAFKA_IMAGE = "confluentinc/cp-kafka:7.5.0"
    DEFAULT_UI_IMAGE = "provectuslabs/kafka-ui:latest"

    def __init__(
        self,
        workspace_name: str,
        stage: str = "production",
        kafka_image: str = "",
        ui_image: str = "",
    ):
        super().__init__(workspace_name, stage)
        self.kafka_image = kafka_image or self.DEFAULT_KAFKA_IMAGE
        self.ui_image = ui_image or self.DEFAULT_UI_IMAGE

    @property
    def service_type(self) -> str:
        return "kafka"

    @property
    def ui_container_name(self) -> str:
        return f"{self.workspace_name}__kafka{self.service_suffix}-ui"

    def _generate_secrets_content(self) -> str:
        admin_password = generate_password()
        ui_password = generate_password()
        kafka_host = self.container_name

        jaas_config = (
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="admin" password="{admin_password}" '
            f'user_admin="{admin_password}";'
        )

        lines = [
            f"KAFKA_ADMIN_PASSWORD={admin_password}",
            f"KAFKA_UI_PASSWORD={ui_password}",
            f"KAFKA_HOSTNAME={kafka_host}",
            f"KAFKA_LISTENER_NAME_SASL_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG='{jaas_config}'",
            f"SPRING_SECURITY_USER_PASSWORD={ui_password}",
            f"KAFKA_CLUSTERS_0_PROPERTIES_SASL_JAAS_CONFIG='{jaas_config}'",
        ]
        return "\n".join(lines) + "\n"

    async def stop(self) -> dict:
        """Stop both Kafka broker and UI containers."""
        result = await super().stop()
        # Also stop the UI container
        try:
            await run_docker_command("docker", "stop", self.ui_container_name)
        except Exception as e:
            logger.warning(f"Failed to stop Kafka UI container: {e}")
        return result

    def _generate_compose_dict(self) -> dict:
        cluster_id = generate_cluster_id()

        return {
            "version": "3",
            "services": {
                f"kafka{self.service_suffix}-ui": {
                    "container_name": self.ui_container_name,
                    "restart": "always",
                    "image": self.ui_image,
                    "environment": {
                        "DYNAMIC_CONFIG_ENABLED": "true",
                        "AUTH_TYPE": "LOGIN_FORM",
                        "SPRING_SECURITY_USER_NAME": "admin",
                        "SERVER_SERVLET_CONTEXTPATH": "/kafka",
                        "KAFKA_CLUSTERS_0_NAME": "local-cluster",
                        "KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS": f"{self.container_name}:9092",
                        "KAFKA_CLUSTERS_0_PROPERTIES_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
                        "KAFKA_CLUSTERS_0_PROPERTIES_SASL_MECHANISM": "PLAIN",
                    },
                    "env_file": [self.secrets_file_path],
                    "networks": ["bitswan_network"],
                },
                f"kafka{self.service_suffix}": {
                    "image": self.kafka_image,
                    "container_name": self.container_name,
                    "entrypoint": ["/bin/bash", "-c", KAFKA_ENTRYPOINT_SCRIPT],
                    "environment": {
                        "KAFKA_NODE_ID": 1,
                        "KAFKA_PROCESS_ROLES": "broker,controller",
                        "KAFKA_CONTROLLER_QUORUM_VOTERS": f"1@{self.container_name}:9094",
                        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
                        "KAFKA_LISTENERS": "SASL_PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094",
                        "KAFKA_ADVERTISED_LISTENERS": f"SASL_PLAINTEXT://{self.container_name}:9092",
                        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT",
                        "KAFKA_INTER_BROKER_LISTENER_NAME": "SASL_PLAINTEXT",
                        "KAFKA_SASL_ENABLED_MECHANISMS": "PLAIN",
                        "KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL": "PLAIN",
                        "KAFKA_OPTS": "-Djava.security.auth.login.config=/etc/kafka/kafka_server_jaas.conf",
                        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": 1,
                        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": 1,
                        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": 1,
                        "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "true",
                        "CLUSTER_ID": cluster_id,
                    },
                    "volumes": [
                        f"{self.volume_name}:/var/lib/kafka/data",
                    ],
                    "env_file": [self.secrets_file_path],
                    "restart": "unless-stopped",
                    "networks": ["bitswan_network"],
                },
            },
            "volumes": {self.volume_name: None},
            "networks": {
                "bitswan_network": {"external": True},
            },
        }

    def _get_caddy_upstream(self) -> str:
        # Kafka UI is the web-accessible service
        return f"{self.ui_container_name}:8080"

    def _get_connection_info(self) -> dict:
        info = {
            "broker": f"{self.container_name}:9092",
            "protocol": "SASL_PLAINTEXT",
        }
        if os.path.exists(self.secrets_file_path):
            with open(self.secrets_file_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("KAFKA_ADMIN_PASSWORD="):
                        info["admin_password"] = line.split("=", 1)[1]
                    elif line.startswith("KAFKA_UI_PASSWORD="):
                        info["ui_password"] = line.split("=", 1)[1]
        if self.gitops_domain:
            info["ui_url"] = f"https://{self.caddy_hostname()}/kafka"
        return info
