"""
MinIO infrastructure service management.

Provides MinIO object storage with MinIO Console web UI, following the same
sidecar pattern as PostgresService (server + UI in one container).
"""

import json
import logging
import os
import shutil
import tempfile
from datetime import datetime

from app.services.infra_service import (
    InfraService,
    generate_password,
    run_docker_command,
)

logger = logging.getLogger(__name__)

DEFAULT_MINIO_IMAGE = "minio/minio:latest"


class MinioService(InfraService):
    """Manages MinIO service deployment."""

    def __init__(
        self,
        workspace_name: str,
        stage: str = "production",
        minio_image: str = "",
    ):
        super().__init__(workspace_name, stage)
        self.minio_image = minio_image or DEFAULT_MINIO_IMAGE

    @property
    def service_type(self) -> str:
        return "minio"

    def _generate_secrets_content(self) -> str:
        root_password = generate_password()
        return (
            f"MINIO_ROOT_USER=admin\n"
            f"MINIO_ROOT_PASSWORD={root_password}\n"
            f"MINIO_HOST={self.container_name}\n"
        )

    def _generate_compose_dict(self) -> dict:
        console_upstream = "http://127.0.0.1:9001"

        minio_entry = {
            "image": self.minio_image,
            "container_name": self.container_name,
            "restart": "unless-stopped",
            "command": "server /data --console-address :9001",
            "env_file": [self.secrets_file_path],
            "volumes": [f"{self.volume_name}-data:/data"],
            "networks": [self._get_stage_network()],
            "labels": {},
        }

        # OAuth2-proxy injection for MinIO Console
        if self.oauth2_enabled:
            minio_entry["environment"] = self._get_oauth2_env_vars(console_upstream)
            minio_entry["labels"]["gitops.oauth2.enabled"] = "true"
            minio_entry["labels"]["gitops.oauth2.upstream"] = console_upstream

        return {
            "services": {
                f"minio{self.service_suffix}": minio_entry,
            },
            "volumes": {f"{self.volume_name}-data": None},
            "networks": {
                self._get_stage_network(): {"external": True},
            },
        }

    def _get_caddy_upstream(self) -> str:
        # When oauth2-proxy is active, route through it (port 9999)
        if self.oauth2_enabled:
            return f"{self.container_name}:9999"
        # MinIO Console runs on port 9001
        return f"{self.container_name}:9001"

    def _get_connection_info(self) -> dict:
        info = {}
        if os.path.exists(self.secrets_file_path):
            with open(self.secrets_file_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("MINIO_ROOT_USER="):
                        info["access_key"] = line.split("=", 1)[1]
                    elif line.startswith("MINIO_ROOT_PASSWORD="):
                        info["secret_key"] = line.split("=", 1)[1]
                        info["password"] = line.split("=", 1)[1]
                    elif line.startswith("MINIO_HOST="):
                        info["host"] = line.split("=", 1)[1]
        info["api_port"] = 9000
        info["console_port"] = 9001
        if info.get("host"):
            info["endpoint"] = f"http://{info['host']}:9000"
        if self.gitops_domain:
            info["admin_ui"] = f"https://{self.caddy_hostname()}"
        return info

    async def backup(self, backup_path: str) -> dict:
        """Backup MinIO data using mc mirror."""
        if not self.is_enabled():
            raise ValueError(f"{self.display_name} is not enabled")
        if not await self.is_running():
            raise ValueError(f"{self.display_name} is not running")

        info = self._get_connection_info()
        access_key = info.get("access_key", "admin")
        secret_key = info.get("secret_key", "")

        temp_dir = tempfile.mkdtemp(prefix="minio-backup-")
        try:
            # Configure mc alias inside the container
            stdout, stderr, rc = await run_docker_command(
                "docker",
                "exec",
                self.container_name,
                "mc",
                "alias",
                "set",
                "local",
                "http://localhost:9000",
                access_key,
                secret_key,
            )
            if rc != 0:
                raise RuntimeError(f"mc alias set failed: {stderr}")

            # List all buckets
            stdout, stderr, rc = await run_docker_command(
                "docker",
                "exec",
                self.container_name,
                "mc",
                "ls",
                "local",
                "--json",
            )
            if rc != 0:
                raise RuntimeError(f"mc ls failed: {stderr}")

            buckets = []
            for line in stdout.strip().split("\n"):
                if line.strip():
                    try:
                        entry = json.loads(line)
                        if entry.get("type") == "folder":
                            bucket_name = entry.get("key", "").rstrip("/")
                            if bucket_name:
                                buckets.append(bucket_name)
                    except json.JSONDecodeError:
                        continue

            # Mirror each bucket to a temp location inside the container
            backup_container_dir = "/tmp/minio-backup"
            await run_docker_command(
                "docker",
                "exec",
                self.container_name,
                "rm",
                "-rf",
                backup_container_dir,
            )
            await run_docker_command(
                "docker",
                "exec",
                self.container_name,
                "mkdir",
                "-p",
                backup_container_dir,
            )

            for bucket in buckets:
                stdout, stderr, rc = await run_docker_command(
                    "docker",
                    "exec",
                    self.container_name,
                    "mc",
                    "mirror",
                    f"local/{bucket}",
                    f"{backup_container_dir}/{bucket}",
                )
                if rc != 0:
                    logger.warning(f"Failed to mirror bucket {bucket}: {stderr}")

            # Create manifest
            backup_time = datetime.utcnow()
            manifest = {
                "version": 1,
                "workspace": self.workspace_name,
                "backup_date": backup_time.isoformat(),
                "format": "mc_mirror",
                "buckets": buckets,
                "minio_host": self.container_name,
            }
            manifest_json = json.dumps(manifest, indent=2)

            # Write manifest inside container
            await run_docker_command(
                "docker",
                "exec",
                self.container_name,
                "sh",
                "-c",
                f"cat > {backup_container_dir}/manifest.json << 'MANIFESTEOF'\n{manifest_json}\nMANIFESTEOF",
            )

            # Create tarball inside container and copy it out
            backup_prefix = f"minio{self.service_suffix}"
            tarball_name = (
                f"{backup_prefix}-backup-{backup_time.strftime('%Y%m%d-%H%M%S')}.tar.gz"
            )
            tarball_container_path = f"/tmp/{tarball_name}"

            stdout, stderr, rc = await run_docker_command(
                "docker",
                "exec",
                self.container_name,
                "tar",
                "-czf",
                tarball_container_path,
                "-C",
                backup_container_dir,
                ".",
            )
            if rc != 0:
                raise RuntimeError(f"tar failed: {stderr}")

            os.makedirs(backup_path, exist_ok=True)
            tarball_path = os.path.join(backup_path, tarball_name)

            stdout, stderr, rc = await run_docker_command(
                "docker",
                "cp",
                f"{self.container_name}:{tarball_container_path}",
                tarball_path,
            )
            if rc != 0:
                raise RuntimeError(f"docker cp failed: {stderr}")

            # Cleanup inside container
            await run_docker_command(
                "docker",
                "exec",
                self.container_name,
                "rm",
                "-rf",
                backup_container_dir,
                tarball_container_path,
            )

            logger.info(f"Backup completed: {tarball_path}")
            return {
                "status": "ok",
                "backup_path": tarball_path,
                "tarball": tarball_name,
                "buckets": buckets,
            }
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    async def restore(self, backup_path: str, force: bool = False) -> dict:
        """Restore MinIO data from a backup tarball."""
        if not self.is_enabled():
            raise ValueError(f"{self.display_name} is not enabled")
        if not await self.is_running():
            raise ValueError(f"{self.display_name} is not running")

        info = self._get_connection_info()
        access_key = info.get("access_key", "admin")
        secret_key = info.get("secret_key", "")

        # Configure mc alias
        stdout, stderr, rc = await run_docker_command(
            "docker",
            "exec",
            self.container_name,
            "mc",
            "alias",
            "set",
            "local",
            "http://localhost:9000",
            access_key,
            secret_key,
        )
        if rc != 0:
            raise RuntimeError(f"mc alias set failed: {stderr}")

        restore_container_dir = "/tmp/minio-restore"

        if os.path.isfile(backup_path) and backup_path.endswith(".tar.gz"):
            # Copy tarball into container and extract
            await run_docker_command(
                "docker",
                "exec",
                self.container_name,
                "rm",
                "-rf",
                restore_container_dir,
            )
            await run_docker_command(
                "docker",
                "exec",
                self.container_name,
                "mkdir",
                "-p",
                restore_container_dir,
            )

            stdout, stderr, rc = await run_docker_command(
                "docker",
                "cp",
                backup_path,
                f"{self.container_name}:/tmp/minio-restore.tar.gz",
            )
            if rc != 0:
                raise RuntimeError(f"docker cp failed: {stderr}")

            stdout, stderr, rc = await run_docker_command(
                "docker",
                "exec",
                self.container_name,
                "tar",
                "-xzf",
                "/tmp/minio-restore.tar.gz",
                "-C",
                restore_container_dir,
            )
            if rc != 0:
                raise RuntimeError(f"tar extract failed: {stderr}")
        elif os.path.isdir(backup_path):
            # Copy directory contents into container
            await run_docker_command(
                "docker",
                "exec",
                self.container_name,
                "rm",
                "-rf",
                restore_container_dir,
            )
            stdout, stderr, rc = await run_docker_command(
                "docker",
                "cp",
                backup_path,
                f"{self.container_name}:{restore_container_dir}",
            )
            if rc != 0:
                raise RuntimeError(f"docker cp failed: {stderr}")
        else:
            raise ValueError(
                f"Backup path does not exist or is not a valid format: {backup_path}"
            )

        try:
            # Read manifest to get bucket list
            stdout, stderr, rc = await run_docker_command(
                "docker",
                "exec",
                self.container_name,
                "cat",
                f"{restore_container_dir}/manifest.json",
            )

            buckets = []
            if rc == 0 and stdout.strip():
                try:
                    manifest = json.loads(stdout)
                    buckets = manifest.get("buckets", [])
                except json.JSONDecodeError:
                    pass

            if not buckets:
                # Fallback: list directories in the restore dir
                stdout, stderr, rc = await run_docker_command(
                    "docker",
                    "exec",
                    self.container_name,
                    "ls",
                    "-d",
                    f"{restore_container_dir}/*/",
                )
                if rc == 0:
                    for line in stdout.strip().split("\n"):
                        bucket = line.strip().rstrip("/").split("/")[-1]
                        if bucket and bucket != "manifest.json":
                            buckets.append(bucket)

            # Restore each bucket
            for bucket in buckets:
                # Create bucket if it doesn't exist
                await run_docker_command(
                    "docker",
                    "exec",
                    self.container_name,
                    "mc",
                    "mb",
                    "--ignore-existing",
                    f"local/{bucket}",
                )

                # Mirror data back
                stdout, stderr, rc = await run_docker_command(
                    "docker",
                    "exec",
                    self.container_name,
                    "mc",
                    "mirror",
                    "--overwrite",
                    f"{restore_container_dir}/{bucket}",
                    f"local/{bucket}",
                )
                if rc != 0:
                    logger.warning(f"Failed to restore bucket {bucket}: {stderr}")

            # Cleanup
            await run_docker_command(
                "docker",
                "exec",
                self.container_name,
                "rm",
                "-rf",
                restore_container_dir,
                "/tmp/minio-restore.tar.gz",
            )

            logger.info("MinIO restore completed")
            return {
                "status": "ok",
                "message": "Restore completed successfully",
                "buckets": buckets,
            }
        except Exception:
            # Cleanup on failure
            await run_docker_command(
                "docker",
                "exec",
                self.container_name,
                "rm",
                "-rf",
                restore_container_dir,
                "/tmp/minio-restore.tar.gz",
            )
            raise
