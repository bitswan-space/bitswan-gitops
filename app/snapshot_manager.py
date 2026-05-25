"""
Snapshot manager: tracks in-flight snapshot operations with per-task locking.

Prevents concurrent snapshot operations and broadcasts progress via SSE.
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

logger = logging.getLogger(__name__)


class SnapshotKind(str, Enum):
    CREATE = "create"
    CLONE = "clone"
    DELETE = "delete"


class SnapshotStep(str, Enum):
    QUEUED = "queued"
    ESTIMATING_SIZE = "estimating_size"
    DISK_CHECK = "disk_check"
    PREPARING_DIR = "preparing_dir"
    BACKUP_POSTGRES = "backup_postgres"
    BACKUP_COUCHDB = "backup_couchdb"
    BACKUP_MINIO = "backup_minio"
    WRITING_MANIFEST = "writing_manifest"
    STOPPING_TARGET_AUTOMATIONS = "stopping_target_automations"
    RESTORE_POSTGRES = "restore_postgres"
    RESTORE_COUCHDB = "restore_couchdb"
    RESTORE_MINIO = "restore_minio"
    STARTING_TARGET_AUTOMATIONS = "starting_target_automations"
    CLEANUP_OLD = "cleanup_old"
    DONE = "done"
    FAILED = "failed"


@dataclass
class SnapshotTask:
    task_id: str
    kind: SnapshotKind
    snapshot_id: str
    source_stage: str
    target_stage: str | None = None
    status: str = "pending"
    step: SnapshotStep | None = None
    message: str = ""
    error: str | None = None
    per_service_errors: dict[str, str | None] = field(default_factory=dict)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    bytes_done: int = 0
    bytes_total: int = 0

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "kind": self.kind.value,
            "snapshot_id": self.snapshot_id,
            "source_stage": self.source_stage,
            "target_stage": self.target_stage,
            "status": self.status,
            "step": self.step.value if self.step else None,
            "message": self.message,
            "error": self.error,
            "per_service_errors": self.per_service_errors,
            "started_at": self.started_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "bytes_done": self.bytes_done,
            "bytes_total": self.bytes_total,
        }


class SnapshotManager:
    def __init__(self):
        self._active_tasks: dict[str, SnapshotTask] = {}
        self._lock = asyncio.Lock()

    async def create_task(
        self,
        kind: SnapshotKind,
        snapshot_id: str,
        source_stage: str,
        target_stage: str | None = None,
    ) -> SnapshotTask:
        task_id = str(uuid.uuid4())
        task = SnapshotTask(
            task_id=task_id,
            kind=kind,
            snapshot_id=snapshot_id,
            source_stage=source_stage,
            target_stage=target_stage,
            step=SnapshotStep.QUEUED,
        )
        async with self._lock:
            self._active_tasks[task_id] = task
        return task

    async def update_task(
        self,
        task_id: str,
        *,
        status: str | None = None,
        step: SnapshotStep | None = None,
        message: str | None = None,
        error: str | None = None,
        per_service_errors: dict[str, str | None] | None = None,
        bytes_done: int | None = None,
        bytes_total: int | None = None,
    ) -> None:
        async with self._lock:
            task = self._active_tasks.get(task_id)
            if not task:
                return
            if status is not None:
                task.status = status
            if step is not None:
                task.step = step
            if message is not None:
                task.message = message
            if error is not None:
                task.error = error
            if per_service_errors is not None:
                task.per_service_errors = per_service_errors
            if bytes_done is not None:
                task.bytes_done = bytes_done
            if bytes_total is not None:
                task.bytes_total = bytes_total
            task.updated_at = datetime.now(timezone.utc)

    def get_task(self, task_id: str) -> SnapshotTask | None:
        return self._active_tasks.get(task_id)

    def get_active_for_stage(self, stage: str) -> list[SnapshotTask]:
        """Return running/pending tasks that target or source the given stage."""
        return [
            t
            for t in self._active_tasks.values()
            if t.status in ("pending", "running")
            and (t.target_stage == stage or t.source_stage == stage)
        ]

    def to_dict_all(self) -> list[dict]:
        return [t.to_dict() for t in self._active_tasks.values()]

    def cleanup_old_tasks(self, max_age_seconds: int = 3600) -> None:
        now = datetime.now(timezone.utc)
        to_remove = [
            tid
            for tid, task in self._active_tasks.items()
            if task.status in ("success", "error")
            and (now - task.started_at).total_seconds() > max_age_seconds
        ]
        for tid in to_remove:
            del self._active_tasks[tid]
        if to_remove:
            logger.info("Cleaned up %d old snapshot tasks", len(to_remove))


snapshot_manager = SnapshotManager()
