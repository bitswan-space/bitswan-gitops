"""
Deploy manager: tracks in-flight deploys with per-deployment locking.

Prevents concurrent deploys of the same deployment_id and broadcasts
progress via SSE.
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

logger = logging.getLogger(__name__)


class DeployStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class DeployStep(str, Enum):
    UPDATING_CONFIG = "updating_config"
    ENABLING_SERVICES = "enabling_services"
    GENERATING_COMPOSE = "generating_compose"
    DOCKER_COMPOSE_UP = "docker_compose_up"
    INSTALLING_CERTS = "installing_certs"
    STARTING_OAUTH2_PROXY = "starting_oauth2_proxy"
    STORING_TAGS = "storing_tags"
    DONE = "done"


@dataclass
class DeployTask:
    task_id: str
    deployment_id: str
    status: DeployStatus = DeployStatus.PENDING
    step: DeployStep | None = None
    message: str = ""
    error: str | None = None
    build_checksum: str | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "deployment_id": self.deployment_id,
            "status": self.status.value,
            "step": self.step.value if self.step else None,
            "message": self.message,
            "error": self.error,
            "build_checksum": self.build_checksum,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat()
            if self.completed_at
            else None,
        }


class DeployManager:
    def __init__(self):
        self._tasks: dict[str, DeployTask] = {}  # task_id → task
        self._active_deploys: dict[str, str] = {}  # deployment_id → task_id
        self._lock = asyncio.Lock()

    def is_deploying(self, deployment_id: str) -> bool:
        return deployment_id in self._active_deploys

    async def create_task(self, deployment_id: str) -> DeployTask | None:
        """Create a deploy task. Returns None if deployment_id is already deploying."""
        async with self._lock:
            if deployment_id in self._active_deploys:
                return None
            task_id = str(uuid.uuid4())
            task = DeployTask(task_id=task_id, deployment_id=deployment_id)
            self._tasks[task_id] = task
            self._active_deploys[deployment_id] = task_id
            return task

    async def update_task(
        self,
        task_id: str,
        status: DeployStatus | None = None,
        step: DeployStep | None = None,
        message: str | None = None,
        error: str | None = None,
    ):
        task = self._tasks.get(task_id)
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
        if status in (DeployStatus.COMPLETED, DeployStatus.FAILED):
            task.completed_at = datetime.now(timezone.utc)
            async with self._lock:
                self._active_deploys.pop(task.deployment_id, None)

    def get_task(self, task_id: str) -> DeployTask | None:
        return self._tasks.get(task_id)

    def get_all_active_tasks(self) -> list[DeployTask]:
        return [
            self._tasks[tid]
            for tid in self._active_deploys.values()
            if tid in self._tasks
        ]

    def cleanup_old_tasks(self, max_age_seconds: int = 3600):
        now = datetime.now(timezone.utc)
        to_remove = []
        for task_id, task in self._tasks.items():
            if task.status in (DeployStatus.COMPLETED, DeployStatus.FAILED):
                age = (now - task.started_at).total_seconds()
                if age > max_age_seconds:
                    to_remove.append(task_id)
        for task_id in to_remove:
            del self._tasks[task_id]
        if to_remove:
            logger.info("Cleaned up %d old deploy tasks", len(to_remove))


# Singleton
deploy_manager = DeployManager()
