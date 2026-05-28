"""
Snapshot service: creates, lists, clones and deletes per-stage data snapshots.

Snapshots capture Postgres + CouchDB + MinIO from a source stage and let
operators restore the data into a target stage (primarily prod → dev/staging).

Also supports per-worktree Postgres-only snapshots (source_kind="worktree"),
restricted to the dev service realm.
"""

import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

from app.services.infra_service import get_service, run_docker_command
from app.snapshot_manager import (
    SnapshotKind,
    SnapshotManager,
    SnapshotStep,
    SnapshotTask,
)
from app.utils import worktree_db_name

logger = logging.getLogger(__name__)

GITOPS_VERSION = "0.1.0"


def _utc_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _short_uuid() -> str:
    return uuid.uuid4().hex[:8]


def _sanitize_worktree_for_id(name: str) -> str:
    """Filesystem-safe slug for embedding a worktree name in snapshot_id."""
    return re.sub(r"[^a-z0-9-]", "-", name.lower()).strip("-") or "worktree"


def _deployment_belongs_to_worktree(dep_conf: dict | None, worktree: str) -> bool:
    """Match a bitswan.yaml deployment entry to a worktree via its relative_path."""
    rel = (dep_conf or {}).get("relative_path") or ""
    if not rel:
        return False
    parts = rel.replace("\\", "/").lstrip("/").split("/")
    return len(parts) >= 2 and parts[0] == "worktrees" and parts[1] == worktree


class SnapshotService:
    def __init__(
        self,
        workspace_name: str,
        snapshot_manager: SnapshotManager,
        snapshots_dir: Path,
        retention_per_stage: int = 5,
    ):
        self.workspace_name = workspace_name
        self.snapshot_manager = snapshot_manager
        self.snapshots_dir = snapshots_dir
        self.retention_per_stage = retention_per_stage
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Estimate
    # ------------------------------------------------------------------

    async def estimate_size(self, stage: str, worktree: str | None = None) -> dict:
        """Return rough byte sizes for postgres/couchdb/minio on a stage.

        When `worktree` is set, only the per-worktree Postgres DB is measured;
        the dev realm is assumed and couchdb/minio bytes are 0 (shared, not
        snapshotted for worktree-scoped operations).
        """
        if worktree:
            if worktree not in self._list_worktrees():
                raise ValueError(f"Worktree '{worktree}' not found")
            pg_svc = get_service("postgres", self.workspace_name, "dev")
            postgres_bytes = await self._estimate_postgres_db(
                pg_svc, worktree_db_name(worktree)
            )
            return {
                "postgres": postgres_bytes,
                "couchdb": 0,
                "minio": 0,
                "total": postgres_bytes,
            }

        pg_svc = get_service("postgres", self.workspace_name, stage)
        couch_svc = get_service("couchdb", self.workspace_name, stage)
        minio_svc = get_service("minio", self.workspace_name, stage)

        postgres_bytes = await self._estimate_postgres(pg_svc)
        couchdb_bytes = await self._estimate_couchdb(couch_svc)
        minio_bytes = await self._estimate_minio(minio_svc)

        total = postgres_bytes + couchdb_bytes + minio_bytes
        return {
            "postgres": postgres_bytes,
            "couchdb": couchdb_bytes,
            "minio": minio_bytes,
            "total": total,
        }

    async def _estimate_postgres_db(self, svc, db_name: str) -> int:
        """Byte size of a single Postgres database."""
        try:
            if not svc.is_enabled() or not await svc.is_running():
                return 0
            stdout, _, rc = await run_docker_command(
                "docker",
                "exec",
                svc.container_name,
                "psql",
                "-U",
                "admin",
                "-tAc",
                f"SELECT COALESCE(pg_database_size('{db_name}'),0);",
            )
            return int(stdout.strip()) if rc == 0 and stdout.strip().isdigit() else 0
        except Exception:
            return 0

    async def _estimate_postgres(self, svc) -> int:
        try:
            if not svc.is_enabled() or not await svc.is_running():
                return 0
            stdout, _, rc = await run_docker_command(
                "docker",
                "exec",
                svc.container_name,
                "psql",
                "-U",
                "admin",
                "-tAc",
                "SELECT COALESCE(SUM(pg_database_size(datname)),0) FROM pg_database"
                " WHERE datname NOT IN ('template0','template1','postgres');",
            )
            return int(stdout.strip()) if rc == 0 and stdout.strip().isdigit() else 0
        except Exception:
            return 0

    async def _estimate_couchdb(self, svc) -> int:
        try:
            if not svc.is_enabled() or not await svc.is_running():
                return 0
            stdout, _, rc = await run_docker_command(
                "docker",
                "exec",
                svc.container_name,
                "du",
                "-sb",
                "/opt/couchdb/data",
            )
            if rc == 0 and stdout.strip():
                return int(stdout.strip().split()[0])
            return 0
        except Exception:
            return 0

    async def _estimate_minio(self, svc) -> int:
        try:
            if not svc.is_enabled() or not await svc.is_running():
                return 0
            info = svc._get_connection_info()
            access_key = info.get("access_key", "admin")
            secret_key = info.get("secret_key", "")
            # Set alias
            await run_docker_command(
                "docker",
                "exec",
                svc.container_name,
                "mc",
                "alias",
                "set",
                "local",
                "http://localhost:9000",
                access_key,
                secret_key,
            )
            # List buckets
            stdout, _, rc = await run_docker_command(
                "docker",
                "exec",
                svc.container_name,
                "mc",
                "ls",
                "local",
                "--json",
            )
            if rc != 0:
                return 0
            total = 0
            buckets = []
            for line in stdout.strip().split("\n"):
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line)
                    if entry.get("type") == "folder":
                        bname = entry.get("key", "").rstrip("/")
                        if bname:
                            buckets.append(bname)
                except json.JSONDecodeError:
                    continue
            for bucket in buckets:
                du_out, _, du_rc = await run_docker_command(
                    "docker",
                    "exec",
                    svc.container_name,
                    "mc",
                    "du",
                    "--json",
                    f"local/{bucket}",
                )
                if du_rc == 0 and du_out.strip():
                    try:
                        data = json.loads(du_out.strip().split("\n")[-1])
                        total += int(data.get("size", 0))
                    except (json.JSONDecodeError, ValueError):
                        pass
            return total
        except Exception:
            return 0

    # ------------------------------------------------------------------
    # Worktrees (helpers shared with create/clone paths)
    # ------------------------------------------------------------------

    def _list_worktrees(self) -> list[str]:
        """Enumerate worktree names from disk (no git shell-out)."""
        base = os.environ.get("BITSWAN_WORKSPACE_REPO_DIR", "/workspace-repo")
        worktrees_dir = os.path.join(base, "worktrees")
        if not os.path.isdir(worktrees_dir):
            return []
        names: list[str] = []
        for entry in os.listdir(worktrees_dir):
            if entry.startswith("."):
                continue
            if os.path.isdir(os.path.join(worktrees_dir, entry)):
                names.append(entry)
        return sorted(names)

    # ------------------------------------------------------------------
    # Create
    # ------------------------------------------------------------------

    async def create_snapshot(
        self,
        source_stage: str,
        name: str | None = None,
        worktree: str | None = None,
    ) -> str:
        """Start a create-snapshot task. Returns task_id.

        When `worktree` is set, runs the per-worktree Postgres-only path on the
        dev service realm; `source_stage` is ignored/forced to 'dev'.
        """
        if worktree:
            if worktree not in self._list_worktrees():
                raise ValueError(f"Worktree '{worktree}' not found")
            slug = _sanitize_worktree_for_id(worktree)
            snapshot_id = f"wt-{slug}-{_utc_compact()}-{_short_uuid()}"
            async with self._lock:
                task = await self.snapshot_manager.create_task(
                    SnapshotKind.CREATE,
                    snapshot_id=snapshot_id,
                    source_stage="dev",
                    worktree=worktree,
                )
                asyncio.create_task(
                    self._run_create_worktree(task, worktree, snapshot_id, name)
                )
                return task.task_id

        snapshot_id = f"{source_stage}-{_utc_compact()}-{_short_uuid()}"

        async with self._lock:
            task = await self.snapshot_manager.create_task(
                SnapshotKind.CREATE,
                snapshot_id=snapshot_id,
                source_stage=source_stage,
            )
            asyncio.create_task(self._run_create(task, source_stage, snapshot_id, name))
            return task.task_id

    async def _run_create(
        self,
        task: SnapshotTask,
        stage: str,
        snapshot_id: str,
        name: str | None,
    ) -> None:
        from app.event_broadcaster import event_broadcaster

        async def _broadcast():
            await event_broadcaster.broadcast("snapshot_progress", task.to_dict())

        async def _step(step: SnapshotStep, msg: str = ""):
            await self.snapshot_manager.update_task(
                task.task_id, status="running", step=step, message=msg
            )
            await _broadcast()

        partial_dir = self.snapshots_dir / f"{snapshot_id}.partial"
        final_dir = self.snapshots_dir / snapshot_id

        try:
            # ESTIMATING_SIZE
            await _step(SnapshotStep.ESTIMATING_SIZE, "Estimating data size…")
            sizes = await self.estimate_size(stage)

            # DISK_CHECK
            await _step(SnapshotStep.DISK_CHECK, "Checking available disk space…")
            self.snapshots_dir.mkdir(parents=True, exist_ok=True)
            free = shutil.disk_usage(self.snapshots_dir).free
            needed = int(sizes["total"] * 1.2) + 1  # at least 1 to avoid /0
            if needed > free:
                raise RuntimeError(
                    f"Insufficient disk space: need ~{needed} bytes, have {free} bytes"
                )

            # PREPARING_DIR
            await _step(SnapshotStep.PREPARING_DIR, "Preparing snapshot directory…")
            partial_dir.mkdir(parents=True, exist_ok=True)

            pg_svc = get_service("postgres", self.workspace_name, stage)
            couch_svc = get_service("couchdb", self.workspace_name, stage)
            minio_svc = get_service("minio", self.workspace_name, stage)

            # BACKUP_POSTGRES / COUCHDB / MINIO in parallel
            await _step(SnapshotStep.BACKUP_POSTGRES, "Backing up Postgres…")
            results = await asyncio.gather(
                self._backup_one("postgres", pg_svc, partial_dir),
                self._backup_one("couchdb", couch_svc, partial_dir),
                self._backup_one("minio", minio_svc, partial_dir),
                return_exceptions=True,
            )

            per_service_errors: dict[str, str | None] = {}
            actual_sizes: dict[str, int] = {}
            for label, res in zip(["postgres", "couchdb", "minio"], results):
                if isinstance(res, Exception):
                    per_service_errors[label] = str(res)
                    logger.error(
                        "Snapshot %s backup failed for %s: %s", snapshot_id, label, res
                    )
                else:
                    per_service_errors[label] = None
                    actual_sizes[label] = res

            if any(v is not None for v in per_service_errors.values()):
                raise RuntimeError(
                    f"Backup failed for: {[k for k, v in per_service_errors.items() if v]}"
                )

            # WRITING_MANIFEST
            await _step(SnapshotStep.WRITING_MANIFEST, "Writing manifest…")
            file_sizes = {
                label: (partial_dir / f"{label}.tar.gz").stat().st_size
                if (partial_dir / f"{label}.tar.gz").exists()
                else 0
                for label in ["postgres", "couchdb", "minio"]
            }
            file_sizes["total"] = sum(file_sizes.values())
            manifest = {
                "snapshot_id": snapshot_id,
                "name": name,
                "source_stage": stage,
                "workspace": self.workspace_name,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "sizes_bytes": file_sizes,
                "gitops_version": GITOPS_VERSION,
                "known_limitations": [
                    "No write-pause during creation; per-DB best-effort consistency only"
                ],
            }
            with open(partial_dir / "manifest.json", "w") as f:
                json.dump(manifest, f, indent=2)

            # Rename partial → final
            partial_dir.rename(final_dir)

            # CLEANUP_OLD
            await _step(SnapshotStep.CLEANUP_OLD, "Cleaning up old snapshots…")
            await self.cleanup_old_snapshots()

            # DONE
            await self.snapshot_manager.update_task(
                task.task_id,
                status="success",
                step=SnapshotStep.DONE,
                message="Snapshot created successfully",
                per_service_errors=per_service_errors,
            )
            await _broadcast()

        except Exception as exc:
            logger.exception("Snapshot create failed: %s", exc)
            await self.snapshot_manager.update_task(
                task.task_id,
                status="error",
                step=SnapshotStep.FAILED,
                error=str(exc),
                message="Snapshot creation failed",
            )
            await _broadcast()

    async def _backup_one(self, label: str, svc, dest_dir: Path) -> int:
        """Run svc.backup() into a temp dir, then move the tarball to dest_dir/<label>.tar.gz."""
        tmp = tempfile.mkdtemp(prefix=f"snap-{label}-")
        try:
            if not svc.is_enabled():
                # Write an empty placeholder so the manifest has a file entry
                placeholder = dest_dir / f"{label}.tar.gz"
                placeholder.touch()
                return 0
            result = await svc.backup(tmp)
            backup_path = result.get("backup_path")
            if not backup_path or not os.path.exists(backup_path):
                raise RuntimeError(f"backup() returned no file for {label}")
            dest = dest_dir / f"{label}.tar.gz"
            shutil.copy2(backup_path, dest)
            return dest.stat().st_size
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    async def _run_create_worktree(
        self,
        task: SnapshotTask,
        worktree: str,
        snapshot_id: str,
        name: str | None,
    ) -> None:
        from app.event_broadcaster import event_broadcaster

        async def _broadcast():
            await event_broadcaster.broadcast("snapshot_progress", task.to_dict())

        async def _step(step: SnapshotStep, msg: str = ""):
            await self.snapshot_manager.update_task(
                task.task_id, status="running", step=step, message=msg
            )
            await _broadcast()

        partial_dir = self.snapshots_dir / f"{snapshot_id}.partial"
        final_dir = self.snapshots_dir / snapshot_id
        db_name = worktree_db_name(worktree)

        try:
            await _step(SnapshotStep.PREPARING_DIR, "Preparing snapshot directory…")
            self.snapshots_dir.mkdir(parents=True, exist_ok=True)
            partial_dir.mkdir(parents=True, exist_ok=True)

            await _step(
                SnapshotStep.BACKUP_POSTGRES,
                f"Dumping worktree database '{db_name}'…",
            )
            pg_svc = get_service("postgres", self.workspace_name, "dev")
            tmp = tempfile.mkdtemp(prefix="snap-wt-postgres-")
            try:
                result = await pg_svc.backup_db(db_name, tmp)
                backup_path = result.get("backup_path")
                if not backup_path or not os.path.exists(backup_path):
                    raise RuntimeError("backup_db() returned no file")
                shutil.copy2(backup_path, partial_dir / "postgres.tar.gz")
            finally:
                shutil.rmtree(tmp, ignore_errors=True)

            await _step(SnapshotStep.WRITING_MANIFEST, "Writing manifest…")
            pg_size = (
                (partial_dir / "postgres.tar.gz").stat().st_size
                if (partial_dir / "postgres.tar.gz").exists()
                else 0
            )
            manifest = {
                "snapshot_id": snapshot_id,
                "name": name,
                "source_kind": "worktree",
                "source_stage": "dev",
                "worktree": worktree,
                "db_name": db_name,
                "workspace": self.workspace_name,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "sizes_bytes": {"postgres": pg_size, "total": pg_size},
                "gitops_version": GITOPS_VERSION,
                "known_limitations": [
                    "Captures Postgres only; CouchDB and MinIO are shared across worktrees."
                ],
            }
            with open(partial_dir / "manifest.json", "w") as f:
                json.dump(manifest, f, indent=2)

            partial_dir.rename(final_dir)

            await _step(SnapshotStep.CLEANUP_OLD, "Cleaning up old snapshots…")
            await self.cleanup_old_snapshots()

            await self.snapshot_manager.update_task(
                task.task_id,
                status="success",
                step=SnapshotStep.DONE,
                message="Worktree snapshot created successfully",
                per_service_errors={"postgres": None},
            )
            await _broadcast()

        except Exception as exc:
            logger.exception("Worktree snapshot create failed: %s", exc)
            await self.snapshot_manager.update_task(
                task.task_id,
                status="error",
                step=SnapshotStep.FAILED,
                error=str(exc),
                message="Worktree snapshot creation failed",
            )
            await _broadcast()

    # ------------------------------------------------------------------
    # List
    # ------------------------------------------------------------------

    def list_snapshots(self) -> list[dict]:
        if not self.snapshots_dir.exists():
            return []
        snapshots = []
        for entry in self.snapshots_dir.iterdir():
            if entry.is_dir() and not entry.name.endswith(".partial"):
                manifest_path = entry / "manifest.json"
                if manifest_path.exists():
                    try:
                        with open(manifest_path) as f:
                            snapshots.append(json.load(f))
                    except Exception:
                        logger.warning("Could not read manifest at %s", manifest_path)
        snapshots.sort(key=lambda s: s.get("created_at", ""), reverse=True)
        return snapshots

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    async def delete_snapshot(self, snapshot_id: str) -> str:
        """Start a delete-snapshot task. Returns task_id."""
        async with self._lock:
            task = await self.snapshot_manager.create_task(
                SnapshotKind.DELETE,
                snapshot_id=snapshot_id,
                source_stage="",
            )
            asyncio.create_task(self._run_delete(task, snapshot_id))
            return task.task_id

    async def _run_delete(self, task: SnapshotTask, snapshot_id: str) -> None:
        from app.event_broadcaster import event_broadcaster

        snap_dir = self.snapshots_dir / snapshot_id
        try:
            await self.snapshot_manager.update_task(
                task.task_id, status="running", step=SnapshotStep.CLEANUP_OLD
            )
            if snap_dir.exists():
                await asyncio.to_thread(shutil.rmtree, snap_dir)
            await self.snapshot_manager.update_task(
                task.task_id,
                status="success",
                step=SnapshotStep.DONE,
                message="Snapshot deleted",
            )
        except Exception as exc:
            await self.snapshot_manager.update_task(
                task.task_id,
                status="error",
                step=SnapshotStep.FAILED,
                error=str(exc),
            )
        finally:
            await event_broadcaster.broadcast("snapshot_progress", task.to_dict())

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    async def cleanup_old_snapshots(self, keep_per_stage: int | None = None) -> None:
        keep = (
            keep_per_stage if keep_per_stage is not None else self.retention_per_stage
        )
        snapshots = self.list_snapshots()
        by_stage: dict[str, list[dict]] = {}
        for s in snapshots:
            stage = s.get("source_stage", "unknown")
            by_stage.setdefault(stage, []).append(s)

        for stage, stage_snaps in by_stage.items():
            # Already sorted desc by created_at; drop tail beyond keep
            to_delete = stage_snaps[keep:]
            for snap in to_delete:
                sid = snap.get("snapshot_id", "")
                snap_dir = self.snapshots_dir / sid
                if snap_dir.exists():
                    await asyncio.to_thread(shutil.rmtree, snap_dir)
                    logger.info("Retention cleanup: removed snapshot %s", sid)

    # ------------------------------------------------------------------
    # Clone
    # ------------------------------------------------------------------

    async def clone_snapshot(
        self,
        snapshot_id: str,
        target_stage: str | None = None,
        confirm_production: bool = False,
        target_worktree: str | None = None,
    ) -> str:
        """Start a clone-snapshot task. Returns task_id.

        Routes to per-worktree clone when the snapshot manifest is worktree-kind
        and `target_worktree` is supplied; otherwise the existing stage clone.
        """
        from app.deploy_manager import deploy_manager
        from app.utils import SERVICE_REALMS

        # Validate manifest exists
        snapshots = self.list_snapshots()
        manifest = next(
            (s for s in snapshots if s.get("snapshot_id") == snapshot_id), None
        )
        if manifest is None:
            raise ValueError(f"Snapshot '{snapshot_id}' not found")

        is_worktree_snapshot = manifest.get("source_kind") == "worktree"

        if is_worktree_snapshot:
            if target_stage:
                raise ValueError(
                    "Worktree snapshots clone into a worktree, not a stage; "
                    "pass target_worktree instead of target_stage."
                )
            if not target_worktree:
                raise ValueError("Worktree snapshot clone requires target_worktree")
            if target_worktree not in self._list_worktrees():
                raise ValueError(f"Target worktree '{target_worktree}' not found")

            active_snaps = self.snapshot_manager.get_active_for_worktree(
                target_worktree
            )
            if active_snaps:
                raise RuntimeError(
                    f"Target worktree '{target_worktree}' is busy with another snapshot operation"
                )

            async with self._lock:
                task = await self.snapshot_manager.create_task(
                    SnapshotKind.CLONE,
                    snapshot_id=snapshot_id,
                    source_stage="dev",
                    worktree=manifest.get("worktree"),
                    target_worktree=target_worktree,
                )
                asyncio.create_task(
                    self._run_clone_worktree(task, snapshot_id, target_worktree)
                )
                return task.task_id

        if target_worktree:
            raise ValueError(
                "Stage snapshots clone into a stage; "
                "target_worktree is only valid for worktree snapshots."
            )
        if not target_stage:
            raise ValueError("target_stage is required for stage-snapshot clones")

        if target_stage not in SERVICE_REALMS:
            raise ValueError(
                f"Invalid target stage '{target_stage}': must be one of {sorted(SERVICE_REALMS)}"
            )

        if target_stage == "production" and not confirm_production:
            raise ValueError(
                "Cloning into production requires confirm_destination_is_production=true"
            )

        # Cross-exclusion: refuse if target stage has active deploys
        active_deploys = deploy_manager.get_active_for_stage(target_stage)
        if active_deploys:
            raise RuntimeError(
                f"Target stage '{target_stage}' has active deploys; retry after they complete"
            )

        # Cross-exclusion: refuse if another snapshot op is already targeting this stage
        active_snaps = self.snapshot_manager.get_active_for_stage(target_stage)
        if active_snaps:
            raise RuntimeError(
                f"Target stage '{target_stage}' is busy with another snapshot operation"
            )

        async with self._lock:
            task = await self.snapshot_manager.create_task(
                SnapshotKind.CLONE,
                snapshot_id=snapshot_id,
                source_stage=manifest.get("source_stage", ""),
                target_stage=target_stage,
            )
            asyncio.create_task(self._run_clone(task, snapshot_id, target_stage))
            return task.task_id

    async def _run_clone(
        self,
        task: SnapshotTask,
        snapshot_id: str,
        target_stage: str,
    ) -> None:
        from app.event_broadcaster import event_broadcaster

        async def _broadcast():
            await event_broadcaster.broadcast("snapshot_progress", task.to_dict())

        async def _step(step: SnapshotStep, msg: str = ""):
            await self.snapshot_manager.update_task(
                task.task_id, status="running", step=step, message=msg
            )
            await _broadcast()

        snap_dir = self.snapshots_dir / snapshot_id
        per_service_errors: dict[str, str | None] = {}

        try:
            # STOPPING_TARGET_AUTOMATIONS
            await _step(
                SnapshotStep.STOPPING_TARGET_AUTOMATIONS,
                f"Stopping automations in stage '{target_stage}'…",
            )
            await self._stop_stage_automations(target_stage)

            # Restore sequentially to limit blast radius
            for label, step in [
                ("postgres", SnapshotStep.RESTORE_POSTGRES),
                ("couchdb", SnapshotStep.RESTORE_COUCHDB),
                ("minio", SnapshotStep.RESTORE_MINIO),
            ]:
                await _step(step, f"Restoring {label}…")
                tar_path = snap_dir / f"{label}.tar.gz"
                svc = get_service(label, self.workspace_name, target_stage)
                try:
                    if not tar_path.exists() or tar_path.stat().st_size == 0:
                        logger.info("Skipping %s restore: no data in snapshot", label)
                        per_service_errors[label] = None
                        continue
                    if not svc.is_enabled():
                        logger.info(
                            "Skipping %s restore: service not enabled on %s",
                            label,
                            target_stage,
                        )
                        per_service_errors[label] = None
                        continue
                    await svc.restore(str(tar_path), force=True)
                    per_service_errors[label] = None
                except Exception as exc:
                    per_service_errors[label] = str(exc)
                    logger.error(
                        "Clone %s restore failed for %s on %s: %s",
                        snapshot_id,
                        label,
                        target_stage,
                        exc,
                    )

            failed_services = [
                k for k, v in per_service_errors.items() if v is not None
            ]
            if failed_services:
                await self.snapshot_manager.update_task(
                    task.task_id,
                    status="error",
                    step=SnapshotStep.FAILED,
                    error=f"Restore failed for: {failed_services}. Target automations left stopped.",
                    per_service_errors=per_service_errors,
                )
                await _broadcast()
                return

            # STARTING_TARGET_AUTOMATIONS
            await _step(
                SnapshotStep.STARTING_TARGET_AUTOMATIONS,
                f"Restarting automations in stage '{target_stage}'…",
            )
            await self._start_stage_automations(target_stage)

            await self.snapshot_manager.update_task(
                task.task_id,
                status="success",
                step=SnapshotStep.DONE,
                message="Clone completed successfully",
                per_service_errors=per_service_errors,
            )
            await _broadcast()

        except Exception as exc:
            logger.exception("Clone failed: %s", exc)
            await self.snapshot_manager.update_task(
                task.task_id,
                status="error",
                step=SnapshotStep.FAILED,
                error=str(exc),
                per_service_errors=per_service_errors,
            )
            await _broadcast()

    async def _run_clone_worktree(
        self,
        task: SnapshotTask,
        snapshot_id: str,
        target_worktree: str,
    ) -> None:
        from app.event_broadcaster import event_broadcaster

        async def _broadcast():
            await event_broadcaster.broadcast("snapshot_progress", task.to_dict())

        async def _step(step: SnapshotStep, msg: str = ""):
            await self.snapshot_manager.update_task(
                task.task_id, status="running", step=step, message=msg
            )
            await _broadcast()

        snap_dir = self.snapshots_dir / snapshot_id
        tar_path = snap_dir / "postgres.tar.gz"
        target_db = worktree_db_name(target_worktree)
        per_service_errors: dict[str, str | None] = {}

        try:
            await _step(
                SnapshotStep.STOPPING_TARGET_AUTOMATIONS,
                f"Stopping live-dev automations for worktree '{target_worktree}'…",
            )
            await self._stop_worktree_automations(target_worktree)

            await _step(
                SnapshotStep.RESTORE_POSTGRES,
                f"Restoring database '{target_db}'…",
            )
            if not tar_path.exists() or tar_path.stat().st_size == 0:
                raise RuntimeError(
                    "Snapshot has no postgres.tar.gz; cannot restore worktree"
                )
            pg_svc = get_service("postgres", self.workspace_name, "dev")
            try:
                await pg_svc.restore_db(str(tar_path), target_db, force=True)
                per_service_errors["postgres"] = None
            except Exception as exc:
                per_service_errors["postgres"] = str(exc)
                logger.error(
                    "Clone %s restore_db failed for worktree '%s' (db=%s): %s",
                    snapshot_id,
                    target_worktree,
                    target_db,
                    exc,
                )

            failed_services = [
                k for k, v in per_service_errors.items() if v is not None
            ]
            if failed_services:
                await self.snapshot_manager.update_task(
                    task.task_id,
                    status="error",
                    step=SnapshotStep.FAILED,
                    error=(
                        f"Restore failed for: {failed_services}. "
                        f"Target worktree '{target_worktree}' automations left stopped."
                    ),
                    per_service_errors=per_service_errors,
                )
                await _broadcast()
                return

            await _step(
                SnapshotStep.STARTING_TARGET_AUTOMATIONS,
                f"Restarting live-dev automations for worktree '{target_worktree}'…",
            )
            await self._start_worktree_automations(target_worktree)

            await self.snapshot_manager.update_task(
                task.task_id,
                status="success",
                step=SnapshotStep.DONE,
                message="Worktree clone completed successfully",
                per_service_errors=per_service_errors,
            )
            await _broadcast()

        except Exception as exc:
            logger.exception("Worktree clone failed: %s", exc)
            await self.snapshot_manager.update_task(
                task.task_id,
                status="error",
                step=SnapshotStep.FAILED,
                error=str(exc),
                per_service_errors=per_service_errors,
            )
            await _broadcast()

    async def _stop_stage_automations(self, stage: str) -> None:
        """Stop all active automations for a given stage."""
        from app.dependencies import get_automation_service

        automation_svc = get_automation_service()
        active = automation_svc.get_active_automations()
        for dep_id, dep_conf in active.items():
            dep_stage = (dep_conf or {}).get("stage") or "production"
            if dep_stage == "":
                dep_stage = "production"
            # live-dev is a deploy stage that uses the dev service realm
            if dep_stage == stage or (stage == "dev" and dep_stage == "live-dev"):
                try:
                    await automation_svc.stop_automation(dep_id)
                    logger.info("Stopped automation %s for snapshot clone", dep_id)
                except Exception as exc:
                    logger.warning("Could not stop automation %s: %s", dep_id, exc)

    async def _start_stage_automations(self, stage: str) -> None:
        """Restart all automations for a given stage."""
        from app.dependencies import get_automation_service

        automation_svc = get_automation_service()
        active = automation_svc.get_active_automations()
        for dep_id, dep_conf in active.items():
            dep_stage = (dep_conf or {}).get("stage") or "production"
            if dep_stage == "":
                dep_stage = "production"
            if dep_stage == stage or (stage == "dev" and dep_stage == "live-dev"):
                try:
                    await automation_svc.restart_automation(dep_id)
                    logger.info("Restarted automation %s after clone", dep_id)
                except Exception as exc:
                    logger.warning("Could not restart automation %s: %s", dep_id, exc)

    async def _stop_worktree_automations(self, worktree: str) -> None:
        """Stop only the live-dev automations tied to a specific worktree."""
        from app.dependencies import get_automation_service

        automation_svc = get_automation_service()
        active = automation_svc.get_active_automations()
        for dep_id, dep_conf in active.items():
            dep_stage = (dep_conf or {}).get("stage") or "production"
            if dep_stage != "live-dev":
                continue
            if not _deployment_belongs_to_worktree(dep_conf, worktree):
                continue
            try:
                await automation_svc.stop_automation(dep_id)
                logger.info(
                    "Stopped automation %s for worktree '%s' clone", dep_id, worktree
                )
            except Exception as exc:
                logger.warning("Could not stop automation %s: %s", dep_id, exc)

    async def _start_worktree_automations(self, worktree: str) -> None:
        """Restart the live-dev automations tied to a specific worktree."""
        from app.dependencies import get_automation_service

        automation_svc = get_automation_service()
        active = automation_svc.get_active_automations()
        for dep_id, dep_conf in active.items():
            dep_stage = (dep_conf or {}).get("stage") or "production"
            if dep_stage != "live-dev":
                continue
            if not _deployment_belongs_to_worktree(dep_conf, worktree):
                continue
            try:
                await automation_svc.restart_automation(dep_id)
                logger.info(
                    "Restarted automation %s for worktree '%s' after clone",
                    dep_id,
                    worktree,
                )
            except Exception as exc:
                logger.warning("Could not restart automation %s: %s", dep_id, exc)

    async def resume_target_automations(
        self,
        task_id: str,
        target_stage: str | None = None,
        target_worktree: str | None = None,
    ) -> None:
        """Restart target automations after an operator-investigated partial clone failure.

        Dispatches to the worktree-scoped restart when target_worktree is set,
        otherwise the stage-scoped restart.
        """
        from app.event_broadcaster import event_broadcaster

        try:
            if target_worktree:
                await self._start_worktree_automations(target_worktree)
                msg = f"Target worktree '{target_worktree}' automations restarted"
            elif target_stage:
                await self._start_stage_automations(target_stage)
                msg = f"Target automations in '{target_stage}' restarted"
            else:
                raise ValueError(
                    "resume_target_automations requires target_stage or target_worktree"
                )
            await self.snapshot_manager.update_task(
                task_id,
                status="success",
                step=SnapshotStep.DONE,
                message=msg,
            )
        except Exception as exc:
            await self.snapshot_manager.update_task(task_id, error=str(exc))
        finally:
            task = self.snapshot_manager.get_task(task_id)
            if task:
                await event_broadcaster.broadcast("snapshot_progress", task.to_dict())
