"""
Infrastructure service management base for workspaces.

Ported from Go implementations in bitswan-automation-server/internal/services/.
Manages infrastructure services (CouchDB, Kafka) as Docker Compose deployments.
"""

import asyncio
import logging
import os
import secrets
import string
from abc import ABC, abstractmethod

from app.utils import SERVICE_REALMS

logger = logging.getLogger(__name__)


def validate_stage(stage: str) -> None:
    """Validate that the given stage is a valid service realm."""
    if stage not in SERVICE_REALMS:
        raise ValueError(
            f"Invalid stage '{stage}': must be one of {sorted(SERVICE_REALMS)}"
        )


def generate_password(length: int = 32) -> str:
    """Generate a random alphanumeric password."""
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


async def run_docker_command(*args: str, cwd: str | None = None) -> tuple[str, str, int]:
    """Run a docker command asynchronously, return (stdout, stderr, returncode)."""
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
    )
    stdout, stderr = await proc.communicate()
    return stdout.decode(), stderr.decode(), proc.returncode


class InfraService(ABC):
    """Base class for infrastructure services (CouchDB, Kafka, etc.)."""

    def __init__(self, workspace_name: str, stage: str = "production"):
        validate_stage(stage)
        self.workspace_name = workspace_name
        self.stage = stage

        # Resolve paths from environment (same as AutomationService)
        bs_home = os.environ.get("BITSWAN_GITOPS_DIR", "/mnt/repo/pipeline")
        bs_home_host = os.environ.get(
            "BITSWAN_GITOPS_DIR_HOST", "/home/root/.config/bitswan/local-gitops/"
        )
        self.secrets_dir = os.path.join(bs_home, "secrets")
        self.secrets_dir_host = os.path.join(bs_home_host, "secrets")
        self.gitops_domain = os.environ.get("BITSWAN_GITOPS_DOMAIN", "")

    @property
    @abstractmethod
    def service_type(self) -> str:
        """Return the service type name (e.g., 'couchdb', 'kafka')."""

    @property
    def service_suffix(self) -> str:
        """Return '-{stage}' for non-production stages, '' for production (backwards compat)."""
        if self.stage == "production":
            return ""
        return f"-{self.stage}"

    @property
    def secrets_file_name(self) -> str:
        """Return the secrets env file name (e.g., 'kafka', 'couchdb-dev', 'kafka-staging')."""
        return f"{self.service_type}{self.service_suffix}"

    @property
    def container_name(self) -> str:
        """Return the main container name (e.g., '{ws}__couchdb-dev')."""
        return f"{self.workspace_name}__{self.service_type}{self.service_suffix}"

    @property
    def project_name(self) -> str:
        """Return the docker-compose project name."""
        return f"{self.workspace_name}-{self.service_type}{self.service_suffix}"

    @property
    def volume_name(self) -> str:
        """Return the Docker volume name."""
        return f"{self.workspace_name}-{self.service_type}{self.service_suffix}-data"

    @property
    def display_name(self) -> str:
        """Return human-readable name (e.g., 'Kafka (dev)')."""
        base = self.service_type.capitalize()
        return f"{base} ({self.stage})"

    def caddy_hostname(self) -> str:
        """Return the Caddy hostname for this service."""
        return f"{self.workspace_name}--{self.service_type}{self.service_suffix}.{self.gitops_domain}"

    @property
    def secrets_file_path(self) -> str:
        return os.path.join(self.secrets_dir, self.secrets_file_name)

    @property
    def secrets_file_path_host(self) -> str:
        return os.path.join(self.secrets_dir_host, self.secrets_file_name)

    def is_enabled(self) -> bool:
        """Check if the service is enabled (secrets file exists)."""
        return os.path.exists(self.secrets_file_path)

    async def is_running(self) -> bool:
        """Check if the service container is running."""
        stdout, _, rc = await run_docker_command(
            "docker", "ps", "--filter", f"name=^/{self.container_name}$",
            "--format", "{{.Names}}"
        )
        return rc == 0 and stdout.strip() != ""

    def _save_secrets(self, content: str) -> None:
        """Save secrets env file."""
        os.makedirs(self.secrets_dir, mode=0o700, exist_ok=True)
        with open(self.secrets_file_path, "w") as f:
            f.write(content)
        os.chmod(self.secrets_file_path, 0o600)
        logger.info(f"{self.display_name} secrets saved to: {self.secrets_file_path}")

    @abstractmethod
    def _generate_secrets_content(self) -> str:
        """Generate secrets file content. Returns the content string."""

    @abstractmethod
    def _generate_compose_dict(self) -> dict:
        """Generate docker-compose dict structure.

        Called by AutomationService.generate_docker_compose() to merge this
        service's entries into the main docker-compose.
        """

    @abstractmethod
    def _get_caddy_upstream(self) -> str:
        """Return the upstream address for Caddy (e.g., 'container:5984')."""

    async def _register_with_caddy(self) -> bool:
        """Register this service with Caddy."""
        from app.utils import add_route_to_caddy

        if not self.gitops_domain:
            logger.warning(
                f"No domain configured, skipping Caddy registration for {self.display_name}"
            )
            return False

        hostname = self.caddy_hostname()
        caddy_id = f"svc-{self.service_type}{self.service_suffix}.{self.workspace_name}"
        upstream = self._get_caddy_upstream()

        result = add_route_to_caddy(hostname, caddy_id, upstream)
        if result:
            logger.info(
                f"Registered {self.display_name} with Caddy: {hostname} -> {upstream}"
            )
        else:
            logger.error(f"Failed to register {self.display_name} with Caddy")
        return result

    async def _unregister_from_caddy(self) -> bool:
        """Remove this service from Caddy."""
        if not self.gitops_domain:
            return False

        caddy_url = os.environ.get("CADDY_URL", "http://caddy:2019")
        caddy_id = f"svc-{self.service_type}{self.service_suffix}.{self.workspace_name}"

        import requests as req

        try:
            response = req.delete(f"{caddy_url}/id/{caddy_id}")
            if response.status_code == 200:
                logger.info(f"Unregistered {self.display_name} from Caddy")
                return True
        except Exception as e:
            logger.warning(f"Failed to unregister {self.display_name} from Caddy: {e}")
        return False

    async def enable(self) -> dict:
        """Enable the service: generate secrets, extra setup, register Caddy.

        The container will be started by the main docker-compose managed by
        AutomationService.
        """
        if self.is_enabled():
            raise ValueError(
                f"{self.display_name} is already enabled for workspace '{self.workspace_name}'"
            )

        logger.info(
            f"Enabling {self.display_name} for workspace '{self.workspace_name}'"
        )

        # Generate and save secrets
        secrets_content = self._generate_secrets_content()
        self._save_secrets(secrets_content)

        # Run any extra setup (e.g., JAAS config for Kafka)
        await self._extra_enable_setup()

        # Register with Caddy
        await self._register_with_caddy()

        logger.info(f"{self.display_name} enabled successfully!")
        return {
            "status": "enabled",
            "service": self.service_type,
            "stage": self.stage,
        }

    async def _extra_enable_setup(self) -> None:
        """Hook for extra setup during enable. Override in subclasses."""
        pass

    def ensure_config(self) -> None:
        """Ensure all config files exist for an already-enabled service.

        Called before generating compose dicts to handle migration cases where
        config files moved to a new location. Override in subclasses.
        """
        pass

    async def disable(self) -> dict:
        """Disable the service: stop container, unregister Caddy, remove secrets.

        Works even if the service is not fully enabled (e.g. containers running
        but secrets file missing) — performs best-effort cleanup.
        """
        logger.info(
            f"Disabling {self.display_name} for workspace '{self.workspace_name}'"
        )

        # Stop container via docker stop
        try:
            await self.stop()
        except Exception as e:
            logger.warning(f"Failed to stop {self.display_name}: {e}")

        # Unregister from Caddy
        await self._unregister_from_caddy()

        # Remove secrets file
        if os.path.exists(self.secrets_file_path):
            os.remove(self.secrets_file_path)

        # Run extra cleanup
        await self._extra_disable_cleanup()

        logger.info(f"{self.display_name} disabled successfully!")
        return {
            "status": "disabled",
            "service": self.service_type,
            "stage": self.stage,
        }

    async def _extra_disable_cleanup(self) -> None:
        """Hook for extra cleanup during disable. Override in subclasses."""
        pass

    async def start(self) -> dict:
        """Start the service container via docker start."""
        logger.info(f"Starting {self.display_name} container '{self.container_name}'...")
        stdout, stderr, rc = await run_docker_command(
            "docker", "start", self.container_name
        )
        if rc != 0:
            raise RuntimeError(f"Failed to start {self.display_name}: {stderr}")
        logger.info(f"{self.display_name} started successfully!")
        return {"status": "started", "service": self.service_type}

    async def stop(self) -> dict:
        """Stop the service container via docker stop."""
        logger.info(f"Stopping {self.display_name} container '{self.container_name}'...")
        stdout, stderr, rc = await run_docker_command(
            "docker", "stop", self.container_name
        )
        if rc != 0:
            raise RuntimeError(f"Failed to stop {self.display_name}: {stderr}")
        logger.info(f"{self.display_name} stopped successfully!")
        return {"status": "stopped", "service": self.service_type}

    async def status(self, show_passwords: bool = False) -> dict:
        """Get service status."""
        enabled = self.is_enabled()
        running = await self.is_running() if enabled else False

        result = {
            "service": self.service_type,
            "stage": self.stage,
            "enabled": enabled,
            "running": running,
        }

        if enabled and show_passwords:
            result["connection_info"] = self._get_connection_info()

        return result

    def _get_connection_info(self) -> dict:
        """Return connection info. Override in subclasses for specific info."""
        return {}


# =============================================================================
# Factory
# =============================================================================


def stage_for_deployment(deployment_stage: str) -> str:
    """Map a deployment stage to its service realm.

    live-dev shares the dev realm; all other stages map to themselves.
    """
    if deployment_stage == "live-dev":
        return "dev"
    return deployment_stage


def get_service(
    service_type: str,
    workspace_name: str,
    stage: str = "production",
    **kwargs,
) -> InfraService:
    """Factory function to create the appropriate service instance."""
    if not stage:
        stage = "production"
    if service_type == "couchdb":
        from app.services.couchdb_service import CouchDBService

        return CouchDBService(
            workspace_name, stage, image=kwargs.get("image", "")
        )
    elif service_type == "kafka":
        from app.services.kafka_service import KafkaService

        return KafkaService(
            workspace_name,
            stage,
            kafka_image=kwargs.get("kafka_image", ""),
            ui_image=kwargs.get("ui_image", ""),
        )
    else:
        raise ValueError(f"Unknown service type: {service_type}. Supported: couchdb, kafka")
