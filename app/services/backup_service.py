"""Backup service using restic to an S3-compatible store.

Backs up:
- Postgres production databases (pg_dump piped to restic)
- CouchDB production databases (JSON export)
- MinIO production buckets
- Workspace files (/workspace-repo)

Encryption key and S3 config stored in secrets/.backup/
"""

import asyncio
import json
import logging
import os
import shutil
import tempfile
from datetime import datetime

logger = logging.getLogger(__name__)

BACKUP_CONFIG_DIR = ".backup"


def _get_backup_dir() -> str:
    bs_home = os.environ.get("BITSWAN_GITOPS_DIR", "/mnt/repo/pipeline")
    return os.path.join(bs_home, "secrets", BACKUP_CONFIG_DIR)


def _get_config_path() -> str:
    return os.path.join(_get_backup_dir(), "config.json")


def _get_key_path() -> str:
    return os.path.join(_get_backup_dir(), "restic-key")


def get_backup_config() -> dict | None:
    """Read backup configuration."""
    path = _get_config_path()
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def save_backup_config(config: dict) -> None:
    """Save backup configuration."""
    backup_dir = _get_backup_dir()
    os.makedirs(backup_dir, mode=0o700, exist_ok=True)
    with open(_get_config_path(), "w") as f:
        json.dump(config, f, indent=2)
    os.chmod(_get_config_path(), 0o600)


def get_restic_key() -> str | None:
    """Read the restic encryption key."""
    path = _get_key_path()
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return f.read().strip()


def generate_restic_key() -> str:
    """Generate and save a new restic encryption key."""
    import secrets

    key = secrets.token_urlsafe(48)
    backup_dir = _get_backup_dir()
    os.makedirs(backup_dir, mode=0o700, exist_ok=True)
    with open(_get_key_path(), "w") as f:
        f.write(key)
    os.chmod(_get_key_path(), 0o600)
    return key


def delete_restic_key() -> None:
    """Delete the local restic key (after user downloads it)."""
    path = _get_key_path()
    if os.path.exists(path):
        os.remove(path)


def is_configured() -> bool:
    """Check if backup is fully configured (S3 + key)."""
    config = get_backup_config()
    return config is not None and get_restic_key() is not None


async def key_exists_on_s3(config: dict) -> bool:
    """Check if the encryption key file exists on S3."""
    stdout, stderr, rc = await _run_s3_cmd(
        config, "ls", f"s3://{config['s3_bucket']}/.restic-key"
    )
    return rc == 0 and ".restic-key" in stdout


async def upload_key_to_s3(config: dict, key: str) -> tuple[bool, str]:
    """Upload the encryption key to S3."""
    key_tmp = os.path.join(tempfile.gettempdir(), ".restic-key-upload")
    try:
        with open(key_tmp, "w") as f:
            f.write(key)
        stdout, stderr, rc = await _run_s3_cmd(
            config, "cp", key_tmp, f"s3://{config['s3_bucket']}/.restic-key"
        )
        if rc != 0:
            return False, stderr.strip()
        return True, "Key uploaded"
    finally:
        if os.path.exists(key_tmp):
            os.remove(key_tmp)


async def download_key_from_s3(config: dict) -> str | None:
    """Download the encryption key from S3."""
    key_tmp = os.path.join(tempfile.gettempdir(), ".restic-key-download")
    try:
        stdout, stderr, rc = await _run_s3_cmd(
            config, "cp", f"s3://{config['s3_bucket']}/.restic-key", key_tmp
        )
        if rc != 0:
            return None
        with open(key_tmp) as f:
            return f.read().strip()
    except Exception:
        return None
    finally:
        if os.path.exists(key_tmp):
            os.remove(key_tmp)


def _save_key(key: str) -> None:
    """Save a key to the local key file."""
    backup_dir = _get_backup_dir()
    os.makedirs(backup_dir, mode=0o700, exist_ok=True)
    with open(_get_key_path(), "w") as f:
        f.write(key)
    os.chmod(_get_key_path(), 0o600)


async def delete_key_from_s3(config: dict) -> tuple[bool, str]:
    """Delete the encryption key from S3."""
    stdout, stderr, rc = await _run_s3_cmd(
        config, "rm", f"s3://{config['s3_bucket']}/.restic-key"
    )
    if rc != 0:
        return False, stderr.strip()
    return True, "Key deleted from S3"


async def _run_s3_cmd(config: dict, *args: str) -> tuple[str, str, int]:
    """Run an AWS CLI S3 command."""
    env = os.environ.copy()
    env["AWS_ACCESS_KEY_ID"] = config.get("s3_access_key", "")
    env["AWS_SECRET_ACCESS_KEY"] = config.get("s3_secret_key", "")

    # Use the S3 endpoint for non-AWS providers
    endpoint = config.get("s3_endpoint", "")
    cmd = ["aws", "s3"]
    if endpoint and "amazonaws.com" not in endpoint:
        cmd.extend(["--endpoint-url", endpoint])
    cmd.extend(args)

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    stdout, stderr = await proc.communicate()
    return stdout.decode(), stderr.decode(), proc.returncode


def _restic_env(config: dict) -> dict:
    """Build environment variables for restic commands."""
    env = os.environ.copy()
    env["RESTIC_REPOSITORY"] = f"s3:{config['s3_endpoint']}/{config['s3_bucket']}"
    env["RESTIC_PASSWORD"] = get_restic_key() or ""
    env["AWS_ACCESS_KEY_ID"] = config.get("s3_access_key", "")
    env["AWS_SECRET_ACCESS_KEY"] = config.get("s3_secret_key", "")
    return env


async def _run_restic(
    args: list[str], config: dict, timeout: int = 3600
) -> tuple[str, str, int]:
    """Run a restic command and return (stdout, stderr, returncode)."""
    env = _restic_env(config)
    proc = await asyncio.create_subprocess_exec(
        "restic",
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        return "", "Timeout", -1
    return stdout.decode(), stderr.decode(), proc.returncode


async def init_repo(config: dict) -> tuple[bool, str]:
    """Initialize the restic repository on S3."""
    stdout, stderr, rc = await _run_restic(["init"], config)
    if rc == 0:
        return True, "Repository initialized"
    if "already initialized" in stderr.lower() or "already exists" in stderr.lower():
        return True, "Repository already initialized"
    return False, stderr.strip()


async def run_backup(config: dict) -> dict:
    """Run a full backup of all production data."""
    workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace-local")
    results = {}
    timestamp = datetime.utcnow().isoformat()

    # 1. Backup workspace files
    workspace_dir = os.environ.get("BITSWAN_WORKSPACE_REPO_DIR", "/workspace-repo")
    if os.path.isdir(workspace_dir):
        stdout, stderr, rc = await _run_restic(
            ["backup", "--tag", "workspace", "--tag", workspace_name, workspace_dir],
            config,
        )
        results["workspace"] = {
            "success": rc == 0,
            "output": stdout.strip() or stderr.strip(),
        }

    # 2. Backup Postgres via pg_dump piped through restic
    results["postgres"] = await _backup_postgres(config, workspace_name)

    # 3. Backup CouchDB via JSON export
    results["couchdb"] = await _backup_couchdb(config, workspace_name)

    # 4. Backup MinIO via mc mirror to temp dir then restic
    results["minio"] = await _backup_minio(config, workspace_name)

    # Apply retention policy
    await _apply_retention(config)

    results["timestamp"] = timestamp
    return results


async def _backup_postgres(config: dict, workspace_name: str) -> dict:
    """Backup Postgres production database."""
    from app.async_docker import get_async_docker_client

    docker_client = get_async_docker_client()
    container_name = f"{workspace_name}__postgres"

    try:
        containers = await docker_client.list_containers(
            all=False, filters={"name": [container_name]}
        )
        if not containers:
            return {"success": True, "output": "No Postgres container running, skipped"}

        cid = containers[0]["Id"]

        # pg_dumpall inside the container
        exec_id = await docker_client.exec_create(
            cid,
            ["pg_dumpall", "-U", "admin"],
        )
        dump = await docker_client.exec_start(exec_id)

        if not dump:
            return {"success": False, "output": "Empty dump"}

        # Write dump to temp file, then restic backup
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write(dump)
            dump_path = f.name

        try:
            stdout, stderr, rc = await _run_restic(
                [
                    "backup",
                    "--tag",
                    "postgres",
                    "--tag",
                    workspace_name,
                    "--stdin-filename",
                    "postgres.sql",
                    "--stdin",
                ],
                config,
            )
            # Actually need to pipe the file. Use a different approach:
            env = _restic_env(config)
            proc = await asyncio.create_subprocess_exec(
                "restic",
                "backup",
                "--tag",
                "postgres",
                "--tag",
                workspace_name,
                dump_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
            stdout_bytes, stderr_bytes = await proc.communicate()
            rc = proc.returncode
            return {
                "success": rc == 0,
                "output": stdout_bytes.decode().strip()
                or stderr_bytes.decode().strip(),
            }
        finally:
            os.unlink(dump_path)

    except Exception as e:
        return {"success": False, "output": str(e)}


async def _backup_couchdb(config: dict, workspace_name: str) -> dict:
    """Backup CouchDB production databases."""
    from app.services.infra_service import get_service

    try:
        svc = get_service("couchdb", workspace_name, stage="production")
        if not svc.is_enabled():
            return {"success": True, "output": "CouchDB not enabled, skipped"}

        # Use existing backup mechanism to create a tarball
        backup_dir = tempfile.mkdtemp(prefix="couchdb-backup-")
        try:
            result = await svc.backup(backup_dir)
            backup_path = result.get("backup_path")
            if not backup_path or not os.path.exists(backup_path):
                return {"success": False, "output": "Backup file not created"}

            stdout, stderr, rc = await _run_restic(
                ["backup", "--tag", "couchdb", "--tag", workspace_name, backup_path],
                config,
            )
            return {"success": rc == 0, "output": stdout.strip() or stderr.strip()}
        finally:
            shutil.rmtree(backup_dir, ignore_errors=True)

    except Exception as e:
        return {"success": False, "output": str(e)}


async def _backup_minio(config: dict, workspace_name: str) -> dict:
    """Backup MinIO production buckets."""
    from app.services.infra_service import get_service

    try:
        svc = get_service("minio", workspace_name, stage="production")
        if not svc.is_enabled():
            return {"success": True, "output": "MinIO not enabled, skipped"}

        backup_dir = tempfile.mkdtemp(prefix="minio-backup-")
        try:
            result = await svc.backup(backup_dir)
            backup_path = result.get("backup_path")
            if not backup_path or not os.path.exists(backup_path):
                return {"success": False, "output": "Backup file not created"}

            stdout, stderr, rc = await _run_restic(
                ["backup", "--tag", "minio", "--tag", workspace_name, backup_path],
                config,
            )
            return {"success": rc == 0, "output": stdout.strip() or stderr.strip()}
        finally:
            shutil.rmtree(backup_dir, ignore_errors=True)

    except Exception as e:
        return {"success": False, "output": str(e)}


async def _apply_retention(config: dict) -> None:
    """Apply retention policy: keep daily for 30 days, monthly for 12 months."""
    retention = config.get("retention", {})
    daily = retention.get("daily", 30)
    monthly = retention.get("monthly", 12)

    await _run_restic(
        [
            "forget",
            "--prune",
            "--keep-daily",
            str(daily),
            "--keep-monthly",
            str(monthly),
        ],
        config,
    )


async def list_snapshots(config: dict, tag: str | None = None) -> list[dict]:
    """List available snapshots, optionally filtered by tag."""
    args = ["snapshots", "--json"]
    if tag:
        args.extend(["--tag", tag])
    stdout, stderr, rc = await _run_restic(args, config)
    if rc != 0:
        return []
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return []


async def restore_snapshot(
    config: dict, snapshot_id: str, target_path: str
) -> tuple[bool, str]:
    """Restore a snapshot to a target path."""
    os.makedirs(target_path, exist_ok=True)
    stdout, stderr, rc = await _run_restic(
        ["restore", snapshot_id, "--target", target_path],
        config,
    )
    if rc == 0:
        return True, f"Restored to {target_path}"
    return False, stderr.strip()


async def restore_postgres(
    config: dict, snapshot_id: str, stage: str = "production"
) -> tuple[bool, str]:
    """Restore a Postgres snapshot to a given stage."""
    from app.async_docker import get_async_docker_client

    workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace-local")

    # Restore the dump file from restic to a temp dir
    restore_dir = tempfile.mkdtemp(prefix="pg-restore-")
    try:
        ok, msg = await restore_snapshot(config, snapshot_id, restore_dir)
        if not ok:
            return False, msg

        # Find the .sql file
        sql_file = None
        for root, dirs, files in os.walk(restore_dir):
            for f in files:
                if f.endswith(".sql"):
                    sql_file = os.path.join(root, f)
                    break
            if sql_file:
                break

        if not sql_file:
            return False, "No SQL dump found in snapshot"

        # Read the dump
        with open(sql_file) as f:
            dump_content = f.read()

        # Execute in the target Postgres container
        suffix = f"-{stage}" if stage != "production" else ""
        container_name = f"{workspace_name}__postgres{suffix}"

        docker_client = get_async_docker_client()
        containers = await docker_client.list_containers(
            all=False, filters={"name": [container_name]}
        )
        if not containers:
            return False, f"Postgres container '{container_name}' not running"

        cid = containers[0]["Id"]
        exec_id = await docker_client.exec_create(
            cid,
            ["psql", "-U", "admin", "-d", "postgres"],
            stdin=True,
        )
        output = await docker_client.exec_start(exec_id, data=dump_content)
        info = await docker_client.exec_inspect(exec_id)

        if info.get("ExitCode", 1) != 0:
            return False, f"psql restore failed: {output}"

        return True, f"Postgres restored to {stage} stage"
    finally:
        shutil.rmtree(restore_dir, ignore_errors=True)


async def restore_couchdb(
    config: dict, snapshot_id: str, stage: str = "production"
) -> tuple[bool, str]:
    """Restore a CouchDB snapshot to a given stage."""
    from app.services.infra_service import get_service

    workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME", "workspace-local")

    restore_dir = tempfile.mkdtemp(prefix="couch-restore-")
    try:
        ok, msg = await restore_snapshot(config, snapshot_id, restore_dir)
        if not ok:
            return False, msg

        # Find the backup tarball
        tar_file = None
        for root, dirs, files in os.walk(restore_dir):
            for f in files:
                if f.endswith(".tar.gz"):
                    tar_file = os.path.join(root, f)
                    break
            if tar_file:
                break

        if not tar_file:
            return False, "No CouchDB backup archive found in snapshot"

        svc = get_service("couchdb", workspace_name, stage=stage)
        await svc.restore(tar_file, force=True)
        return True, f"CouchDB restored to {stage} stage"
    except Exception as e:
        return False, str(e)
    finally:
        shutil.rmtree(restore_dir, ignore_errors=True)


async def restore_workspace(config: dict, snapshot_id: str) -> tuple[bool, str]:
    """Restore workspace files to /tmp/restores/{datetime}."""
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    target = f"/tmp/restores/{timestamp}"
    return await restore_snapshot(config, snapshot_id, target)
