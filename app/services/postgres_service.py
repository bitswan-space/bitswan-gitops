"""
PostgreSQL infrastructure service management.

Provides PostgreSQL database with pgAdmin web UI, following the same
sidecar pattern as KafkaService (broker + UI containers).
"""

import asyncio
import json
import logging
import os
import shutil
import tarfile
import tempfile
from datetime import datetime

from app.services.infra_service import (
    InfraService,
    generate_password,
    run_docker_command,
)

logger = logging.getLogger(__name__)

DEFAULT_POSTGRES_IMAGE = "postgres:16"
DEFAULT_PGADMIN_IMAGE = "dpage/pgadmin4:latest"


class PostgresService(InfraService):
    """Manages PostgreSQL service deployment (PostgreSQL + pgAdmin)."""

    def __init__(
        self,
        workspace_name: str,
        stage: str = "production",
        postgres_image: str = "",
        pgadmin_image: str = "",
    ):
        super().__init__(workspace_name, stage)
        self.postgres_image = postgres_image or DEFAULT_POSTGRES_IMAGE
        self.pgadmin_image = pgadmin_image or DEFAULT_PGADMIN_IMAGE

    @property
    def service_type(self) -> str:
        return "postgres"

    @property
    def pgadmin_container_name(self) -> str:
        return f"{self.workspace_name}__postgres{self.service_suffix}-pgadmin"

    def _generate_secrets_content(self) -> str:
        pg_password = generate_password()
        pgadmin_password = generate_password()
        self._generated_password = pg_password
        return (
            f"POSTGRES_USER=admin\n"
            f"POSTGRES_PASSWORD={pg_password}\n"
            f"POSTGRES_HOST={self.container_name}\n"
            f"POSTGRES_DB=postgres\n"
            f"PGADMIN_DEFAULT_EMAIL=admin@local.dev\n"
            f"PGADMIN_DEFAULT_PASSWORD={pgadmin_password}\n"
        )

    def _generate_compose_dict(self) -> dict:
        pgadmin_upstream = "http://127.0.0.1:80"

        pgadmin_entry = {
            "container_name": self.pgadmin_container_name,
            "restart": "unless-stopped",
            "image": self.pgadmin_image,
            "env_file": [self.secrets_file_path],
            "volumes": [
                f"{os.path.join(self.secrets_dir, 'pgadmin-servers.json')}:/pgadmin4/servers.json:ro",
            ],
            "networks": ["bitswan_network"],
            "labels": {},
        }

        # OAuth2-proxy injection for pgAdmin
        # Env vars are set at compose time; the oauth2-proxy binary is copied
        # into the container and started via docker exec after boot.
        if self.oauth2_enabled:
            pgadmin_entry["environment"] = self._get_oauth2_env_vars(pgadmin_upstream)
            pgadmin_entry["labels"]["gitops.oauth2.enabled"] = "true"
            pgadmin_entry["labels"]["gitops.oauth2.upstream"] = pgadmin_upstream

        return {
            "services": {
                f"postgres{self.service_suffix}": {
                    "image": self.postgres_image,
                    "container_name": self.container_name,
                    "restart": "unless-stopped",
                    "env_file": [self.secrets_file_path],
                    "volumes": [f"{self.volume_name}-data:/var/lib/postgresql/data"],
                    "networks": ["bitswan_network"],
                },
                f"postgres{self.service_suffix}-pgadmin": pgadmin_entry,
            },
            "volumes": {f"{self.volume_name}-data": None},
            "networks": {
                "bitswan_network": {"external": True},
            },
        }

    def _get_caddy_upstream(self) -> str:
        # When oauth2-proxy is active, route through it (port 9999)
        if self.oauth2_enabled:
            return f"{self.pgadmin_container_name}:9999"
        return f"{self.pgadmin_container_name}:80"

    def _get_connection_info(self) -> dict:
        info = {}
        if os.path.exists(self.secrets_file_path):
            with open(self.secrets_file_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("POSTGRES_USER="):
                        info["username"] = line.split("=", 1)[1]
                    elif line.startswith("POSTGRES_PASSWORD="):
                        info["password"] = line.split("=", 1)[1]
                    elif line.startswith("POSTGRES_HOST="):
                        info["host"] = line.split("=", 1)[1]
                    elif line.startswith("POSTGRES_DB="):
                        info["database"] = line.split("=", 1)[1]
        info["port"] = 5432
        if (
            info.get("username")
            and info.get("password")
            and info.get("host")
            and info.get("database")
        ):
            info["connection_string"] = (
                f"postgresql://{info['username']}:{info['password']}"
                f"@{info['host']}:5432/{info['database']}"
            )
        if self.gitops_domain:
            info["admin_ui"] = f"https://{self.caddy_hostname()}"
        return info

    async def _extra_enable_setup(self) -> None:
        """Generate servers.json for pgAdmin auto-discovery."""
        servers_json = {
            "Servers": {
                "1": {
                    "Name": "PostgreSQL",
                    "Group": "Servers",
                    "Host": self.container_name,
                    "Port": 5432,
                    "Username": "admin",
                    "MaintenanceDB": "postgres",
                    "SSLMode": "prefer",
                }
            }
        }
        os.makedirs(self.secrets_dir, mode=0o700, exist_ok=True)
        servers_file = os.path.join(self.secrets_dir, "pgadmin-servers.json")
        with open(servers_file, "w") as f:
            json.dump(servers_json, f, indent=2)
        logger.info(f"pgAdmin servers.json saved to: {servers_file}")

    async def start(self) -> dict:
        """Start both PostgreSQL and pgAdmin containers, then oauth2-proxy."""
        result = await super().start()
        try:
            await run_docker_command("docker", "start", self.pgadmin_container_name)
        except Exception as e:
            logger.warning(f"Failed to start pgAdmin container: {e}")
        # Start oauth2-proxy inside pgAdmin container via docker exec
        await self._start_oauth2_proxy_in_container(self.pgadmin_container_name)
        return result

    async def stop(self) -> dict:
        """Stop both PostgreSQL and pgAdmin containers."""
        result = await super().stop()
        try:
            await run_docker_command("docker", "stop", self.pgadmin_container_name)
        except Exception as e:
            logger.warning(f"Failed to stop pgAdmin container: {e}")
        return result

    async def backup(self, backup_path: str) -> dict:
        """Backup all PostgreSQL databases using pg_dumpall."""
        if not self.is_enabled():
            raise ValueError(f"{self.display_name} is not enabled")
        if not await self.is_running():
            raise ValueError(f"{self.display_name} is not running")

        info = self._get_connection_info()
        user = info.get("username", "admin")

        temp_dir = tempfile.mkdtemp(prefix="postgres-backup-")
        try:
            # Run pg_dumpall inside the container
            dump_file = os.path.join(temp_dir, "dump.sql")
            stdout, stderr, rc = await run_docker_command(
                "docker",
                "exec",
                self.container_name,
                "pg_dumpall",
                "-U",
                user,
            )
            if rc != 0:
                raise RuntimeError(f"pg_dumpall failed: {stderr}")

            with open(dump_file, "w") as f:
                f.write(stdout)

            logger.info(f"pg_dumpall completed, dump size: {len(stdout)} bytes")

            # Create manifest
            backup_time = datetime.utcnow()
            manifest = {
                "version": 1,
                "workspace": self.workspace_name,
                "backup_date": backup_time.isoformat(),
                "format": "pg_dumpall",
                "postgres_host": self.container_name,
            }
            with open(os.path.join(temp_dir, "manifest.json"), "w") as f:
                json.dump(manifest, f, indent=2)

            # Create tarball
            backup_prefix = f"postgres{self.service_suffix}"
            tarball_name = (
                f"{backup_prefix}-backup-{backup_time.strftime('%Y%m%d-%H%M%S')}.tar.gz"
            )
            tarball_path = os.path.join(backup_path, tarball_name)

            os.makedirs(backup_path, exist_ok=True)
            with tarfile.open(tarball_path, "w:gz") as tar:
                for item in os.listdir(temp_dir):
                    tar.add(os.path.join(temp_dir, item), arcname=item)

            logger.info(f"Backup completed: {tarball_path}")
            return {
                "status": "ok",
                "backup_path": tarball_path,
                "tarball": tarball_name,
            }
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    async def restore(self, backup_path: str, force: bool = False) -> dict:
        """Restore PostgreSQL databases from a backup tarball."""
        if not self.is_enabled():
            raise ValueError(f"{self.display_name} is not enabled")
        if not await self.is_running():
            raise ValueError(f"{self.display_name} is not running")

        info = self._get_connection_info()
        user = info.get("username", "admin")
        database = info.get("database", "postgres")

        extract_dir = None
        should_cleanup = False

        if os.path.isfile(backup_path) and backup_path.endswith(".tar.gz"):
            extract_dir = tempfile.mkdtemp(prefix="postgres-restore-")
            should_cleanup = True
            with tarfile.open(backup_path, "r:gz") as tar:
                tar.extractall(extract_dir)
        elif os.path.isdir(backup_path):
            extract_dir = backup_path
        else:
            raise ValueError(
                f"Backup path does not exist or is not a valid format: {backup_path}"
            )

        try:
            # Find the SQL dump file
            dump_file = None
            for fname in os.listdir(extract_dir):
                if fname.endswith(".sql"):
                    dump_file = os.path.join(extract_dir, fname)
                    break

            if not dump_file:
                raise ValueError("No .sql dump file found in backup")

            with open(dump_file, "r") as f:
                sql_content = f.read()

            # Execute psql with dump piped to stdin
            proc = await asyncio.create_subprocess_exec(
                "docker",
                "exec",
                "-i",
                self.container_name,
                "psql",
                "-U",
                user,
                "-d",
                database,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout_bytes, stderr_bytes = await proc.communicate(
                input=sql_content.encode()
            )

            if proc.returncode != 0:
                logger.warning(f"psql restore warnings: {stderr_bytes.decode()}")

            logger.info("PostgreSQL restore completed")
            return {
                "status": "ok",
                "message": "Restore completed successfully",
            }
        finally:
            if should_cleanup and extract_dir:
                shutil.rmtree(extract_dir, ignore_errors=True)

    async def clear(self) -> dict:
        """Drop all user-created databases and recreate an empty default database.

        This is a destructive operation — all data in PostgreSQL will be lost.
        """
        if not self.is_enabled():
            raise ValueError(f"{self.display_name} is not enabled")
        if not await self.is_running():
            raise ValueError(f"{self.display_name} is not running")

        info = self._get_connection_info()
        user = info.get("username", "admin")
        database = info.get("database", "postgres")

        # Get list of user databases (exclude template and postgres system DBs)
        stdout, stderr, rc = await run_docker_command(
            "docker",
            "exec",
            self.container_name,
            "psql",
            "-U",
            user,
            "-d",
            "postgres",
            "-t",
            "-A",
            "-c",
            "SELECT datname FROM pg_database WHERE datistemplate = false AND datname != 'postgres';",
        )
        if rc != 0:
            raise RuntimeError(f"Failed to list databases: {stderr}")

        databases = [db.strip() for db in stdout.strip().split("\n") if db.strip()]

        # Drop each user database
        for db_name in databases:
            logger.info(f"Dropping database: {db_name}")
            _, stderr, rc = await run_docker_command(
                "docker",
                "exec",
                self.container_name,
                "psql",
                "-U",
                user,
                "-d",
                "postgres",
                "-c",
                f'DROP DATABASE IF EXISTS "{db_name}";',
            )
            if rc != 0:
                logger.warning(f"Failed to drop database {db_name}: {stderr}")

        # Drop all objects in the default database (tables, sequences, etc.)
        drop_sql = (
            "DO $$ DECLARE r RECORD; BEGIN "
            "FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP "
            "EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE'; "
            "END LOOP; "
            "FOR r IN (SELECT sequencename FROM pg_sequences WHERE schemaname = 'public') LOOP "
            "EXECUTE 'DROP SEQUENCE IF EXISTS ' || quote_ident(r.sequencename) || ' CASCADE'; "
            "END LOOP; "
            "END $$;"
        )
        _, stderr, rc = await run_docker_command(
            "docker",
            "exec",
            self.container_name,
            "psql",
            "-U",
            user,
            "-d",
            database,
            "-c",
            drop_sql,
        )
        if rc != 0:
            logger.warning(f"Failed to clean default database: {stderr}")

        # Recreate the default database's public schema cleanly
        _, _, _ = await run_docker_command(
            "docker",
            "exec",
            self.container_name,
            "psql",
            "-U",
            user,
            "-d",
            database,
            "-c",
            "DROP SCHEMA public CASCADE; CREATE SCHEMA public;",
        )

        logger.info("PostgreSQL clear completed — all data removed")
        return {
            "status": "ok",
            "message": "All PostgreSQL data has been deleted",
        }
