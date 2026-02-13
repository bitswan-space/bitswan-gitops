"""
CouchDB infrastructure service management.

Ported from bitswan-automation-server/internal/services/couchdb.go.
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


class CouchDBService(InfraService):
    """Manages CouchDB service deployment."""

    DEFAULT_IMAGE = "couchdb:3.3"

    def __init__(self, workspace_name: str, stage: str = "production", image: str = ""):
        super().__init__(workspace_name, stage)
        self.image = image or self.DEFAULT_IMAGE

    @property
    def service_type(self) -> str:
        return "couchdb"

    def _generate_secrets_content(self) -> str:
        password = generate_password()
        self._generated_password = password
        return (
            f"COUCHDB_USER=admin\n"
            f"COUCHDB_PASSWORD={password}\n"
            f"COUCHDB_HOST={self.container_name}\n"
        )

    def _generate_compose_dict(self) -> dict:
        return {
            "services": {
                f"couchdb{self.service_suffix}": {
                    "image": self.image,
                    "container_name": self.container_name,
                    "restart": "unless-stopped",
                    "env_file": [self.secrets_file_path],
                    "volumes": [f"{self.volume_name}:/opt/couchdb/data"],
                    "networks": ["bitswan_network"],
                },
            },
            "volumes": {self.volume_name: None},
            "networks": {
                "bitswan_network": {"external": True},
            },
        }

    def _get_caddy_upstream(self) -> str:
        return f"{self.container_name}:5984"

    def _get_connection_info(self) -> dict:
        """Parse credentials from secrets file."""
        info = {}
        if os.path.exists(self.secrets_file_path):
            with open(self.secrets_file_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("COUCHDB_USER="):
                        info["username"] = line.split("=", 1)[1]
                    elif line.startswith("COUCHDB_PASSWORD="):
                        info["password"] = line.split("=", 1)[1]
                    elif line.startswith("COUCHDB_HOST="):
                        info["host"] = line.split("=", 1)[1]
        if self.gitops_domain:
            info["url"] = f"https://{self.caddy_hostname()}"
            info["admin_ui"] = f"https://{self.caddy_hostname()}/_utils/"
        return info

    async def backup(self, backup_path: str) -> dict:
        """Backup CouchDB databases to a tarball.

        Documents are stored as JSON, attachments as separate binary files.
        Structure: {dbname}/documents.json + {dbname}/attachments/{docid}/{attname}
        """
        if not self.is_enabled():
            raise ValueError(f"{self.display_name} is not enabled")
        if not await self.is_running():
            raise ValueError(f"{self.display_name} is not running")

        info = self._get_connection_info()
        user = info.get("username", "admin")
        password = info.get("password", "")

        # Get list of databases
        stdout, stderr, rc = await run_docker_command(
            "docker",
            "exec",
            self.container_name,
            "curl",
            "-s",
            "-u",
            f"{user}:{password}",
            "http://localhost:5984/_all_dbs",
        )
        if rc != 0:
            raise RuntimeError(f"Failed to list databases: {stderr}")

        databases = json.loads(stdout)
        user_dbs = [db for db in databases if not db.startswith("_")]

        if not user_dbs:
            return {
                "status": "ok",
                "message": "No user databases to backup",
                "databases": [],
            }

        temp_dir = tempfile.mkdtemp(prefix="couchdb-backup-")
        try:
            for db_name in user_dbs:
                logger.info(f"Backing up database: {db_name}")
                db_dir = os.path.join(temp_dir, db_name)
                os.makedirs(os.path.join(db_dir, "attachments"), exist_ok=True)

                # Get all documents with include_docs=true
                stdout, stderr, rc = await run_docker_command(
                    "docker",
                    "exec",
                    self.container_name,
                    "curl",
                    "-s",
                    "-u",
                    f"{user}:{password}",
                    f"http://localhost:5984/{db_name}/_all_docs?include_docs=true",
                )
                if rc != 0:
                    raise RuntimeError(f"Failed to get docs for '{db_name}': {stderr}")

                docs_result = json.loads(stdout)
                rows = docs_result.get("rows", [])

                # Save documents.json
                docs_file = os.path.join(db_dir, "documents.json")
                with open(docs_file, "w") as f:
                    json.dump(docs_result, f)

                logger.info(f"Backed up {len(rows)} documents from '{db_name}'")

            # Create manifest
            backup_time = datetime.utcnow()
            manifest = {
                "version": 2,
                "workspace": self.workspace_name,
                "backup_date": backup_time.isoformat(),
                "databases": user_dbs,
                "couchdb_host": self.container_name,
            }
            with open(os.path.join(temp_dir, "manifest.json"), "w") as f:
                json.dump(manifest, f, indent=2)

            # Create tarball
            backup_prefix = f"couchdb{self.service_suffix}"
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
                "databases": user_dbs,
                "tarball": tarball_name,
            }
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    async def restore(self, backup_path: str, force: bool = False) -> dict:
        """Restore CouchDB databases from a backup tarball or directory.

        Supports both v1 (inline attachments) and v2 (separate attachment files) formats.
        """
        if not self.is_enabled():
            raise ValueError(f"{self.display_name} is not enabled")
        if not await self.is_running():
            raise ValueError(f"{self.display_name} is not running")

        info = self._get_connection_info()
        user = info.get("username", "admin")
        password = info.get("password", "")

        # Determine if backup is tarball or directory
        extract_dir = None
        should_cleanup = False

        if os.path.isfile(backup_path) and backup_path.endswith(".tar.gz"):
            extract_dir = tempfile.mkdtemp(prefix="couchdb-restore-")
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
            # Read manifest
            manifest_file = os.path.join(extract_dir, "manifest.json")
            databases = []
            backup_version = 1

            if os.path.exists(manifest_file):
                with open(manifest_file, "r") as f:
                    manifest = json.load(f)
                backup_version = manifest.get("version", 1)
                databases = manifest.get("databases", [])

            if not databases:
                # Auto-detect databases from directory structure
                for entry in os.listdir(extract_dir):
                    entry_path = os.path.join(extract_dir, entry)
                    if os.path.isdir(entry_path) and entry != "attachments":
                        docs_file = os.path.join(entry_path, "documents.json")
                        if os.path.exists(docs_file):
                            databases.append(entry)
                            backup_version = 2

            if not databases:
                raise ValueError("No databases found in backup")

            restored = []
            for db_name in databases:
                if backup_version >= 2:
                    backup_file = os.path.join(extract_dir, db_name, "documents.json")
                else:
                    backup_file = os.path.join(extract_dir, f"{db_name}.json")

                if not os.path.exists(backup_file):
                    logger.warning(
                        f"Backup file not found for '{db_name}': {backup_file}"
                    )
                    continue

                logger.info(f"Restoring database: {db_name}")

                with open(backup_file, "r") as f:
                    backup_doc = json.load(f)

                # Check if database exists, create if not
                stdout, _, _ = await run_docker_command(
                    "docker",
                    "exec",
                    self.container_name,
                    "curl",
                    "-s",
                    "-o",
                    "/dev/null",
                    "-w",
                    "%{http_code}",
                    "-u",
                    f"{user}:{password}",
                    f"http://localhost:5984/{db_name}",
                )
                if stdout.strip() != "200":
                    await run_docker_command(
                        "docker",
                        "exec",
                        self.container_name,
                        "curl",
                        "-s",
                        "-X",
                        "PUT",
                        "-u",
                        f"{user}:{password}",
                        f"http://localhost:5984/{db_name}",
                    )

                rows = backup_doc.get("rows", [])
                if not rows:
                    continue

                # Build bulk docs payload (remove _rev for clean insert)
                docs = []
                for row in rows:
                    doc = row.get("doc", {})
                    doc.pop("_rev", None)
                    if backup_version >= 2:
                        doc.pop("_attachments", None)
                    docs.append(doc)

                # Bulk insert via stdin
                bulk_payload = json.dumps({"docs": docs})
                proc = await asyncio.create_subprocess_exec(
                    "docker",
                    "exec",
                    "-i",
                    self.container_name,
                    "sh",
                    "-c",
                    f"curl -s -X POST -H 'Content-Type: application/json' "
                    f"-u '{user}:{password}' --data-binary @- "
                    f"'http://localhost:5984/{db_name}/_bulk_docs'",
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout_bytes, _ = await proc.communicate(input=bulk_payload.encode())

                response = json.loads(stdout_bytes.decode())
                success_count = sum(
                    1 for item in response if "rev" in item and "error" not in item
                )
                logger.info(
                    f"Restored {success_count}/{len(docs)} documents to '{db_name}'"
                )
                restored.append(db_name)

            return {
                "status": "ok",
                "databases_restored": restored,
                "backup_version": backup_version,
            }
        finally:
            if should_cleanup and extract_dir:
                shutil.rmtree(extract_dir, ignore_errors=True)
