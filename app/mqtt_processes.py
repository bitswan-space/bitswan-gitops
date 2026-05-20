import os
import json
import toml
import re
import base64
import asyncio
import uuid
from typing import Dict, Any, Optional, Match, Coroutine
from paho.mqtt import client as mqtt_client

from .models import (
    ProcessList,
    ProcessInfo,
    ProcessMarkdown,
    encode_pydantic_model,
)
from .utils import read_bitswan_yaml

import logging

logger = logging.getLogger(__name__)

# MQTT topics this service must always be subscribed to.
# Centralized so `on_connect` can re-subscribe after reconnects.
PROCESS_SUBSCRIPTION_TOPICS: tuple[str, ...] = (
    "/processes/c/+/attachments/c/+/gitops-req",
    "/processes/c/+/attachments/c/+/set",
    "/processes/c/+/gitops-req",
    "/processes/c/+/set",
    "/processes/c/+/create",
)


def subscribe_process_topics(client: mqtt_client.Client) -> None:
    """(Re)subscribe to all process-related topics."""
    for topic in PROCESS_SUBSCRIPTION_TOPICS:
        client.subscribe(topic)


class ProcessService:
    def __init__(self):
        self.bs_home = os.environ.get("BITSWAN_GITOPS_DIR", "/mnt/repo/pipeline")
        self.gitops_dir = os.path.join(self.bs_home, "gitops")
        self.workspace_repo_dir = os.environ.get(
            "BITSWAN_WORKSPACE_REPO_DIR", "/workspace-repo"
        )
        # Per-scope cache of discovered processes. Key is the worktree name,
        # or None for the main repo. Kept fresh by the file-system watchers
        # in `lifespan.py` (see `WorkspaceChangeHandler` and
        # `WorktreeChangeHandler`) so REST/SSE consumers don't pay the cost
        # of a filesystem walk on every request.
        self._cache: Dict[Optional[str], Dict[str, ProcessInfo]] = {}

    def _scope_root(self, worktree: Optional[str] = None) -> str:
        """Filesystem root for a discovery scope."""
        if worktree:
            return os.path.join(self.workspace_repo_dir, "worktrees", worktree)
        return self.workspace_repo_dir

    def discover_processes(
        self, worktree: Optional[str] = None
    ) -> Dict[str, ProcessInfo]:
        """Discover business processes in the main repo or a single worktree.

        A directory qualifies as a BP when it contains both `process.toml`
        and `README.md`, and the toml declares a `process-id` (matches the
        existing MQTT contract).
        """
        processes: Dict[str, ProcessInfo] = {}
        root = self._scope_root(worktree)

        if not os.path.exists(root):
            return processes

        for item in os.listdir(root):
            # Never descend into the worktrees tree when scanning the main repo.
            if worktree is None and item == "worktrees":
                continue
            process_path = os.path.join(root, item)
            if not os.path.isdir(process_path):
                continue

            process_toml_path = os.path.join(process_path, "process.toml")
            process_md_path = os.path.join(process_path, "README.md")

            if not (
                os.path.exists(process_toml_path) and os.path.exists(process_md_path)
            ):
                continue

            try:
                with open(process_toml_path, "r") as f:
                    process_config = toml.load(f)
                    process_id = process_config.get("process-id")

                if not process_id:
                    continue

                processes[process_id] = ProcessInfo(
                    id=process_id,
                    name=item,
                    attachments=self.get_process_attachments(process_id),
                    automation_sources=self.get_process_automation_sources(process_id),
                )

            except Exception as e:
                logger.error(
                    f"Error reading process {item} (worktree={worktree or 'main'}): {e}"
                )
                continue

        return processes

    # --- In-memory cache + refresh -----------------------------------------

    def refresh(self, worktree: Optional[str] = None) -> Dict[str, ProcessInfo]:
        """Re-scan one scope and update the cache. Returns the new mapping."""
        result = self.discover_processes(worktree)
        self._cache[worktree] = result
        return result

    def refresh_all(self) -> None:
        """Warm the cache from scratch: main repo + every worktree on disk."""
        self.refresh(None)
        worktrees_root = os.path.join(self.workspace_repo_dir, "worktrees")
        if not os.path.isdir(worktrees_root):
            # Drop any stale worktree entries (e.g. all worktrees removed).
            for key in [k for k in self._cache.keys() if k is not None]:
                self._cache.pop(key, None)
            return
        live = set()
        for entry in os.listdir(worktrees_root):
            if entry.startswith("."):
                continue
            full = os.path.join(worktrees_root, entry)
            if not os.path.isdir(full):
                continue
            live.add(entry)
            self.refresh(entry)
        # Forget worktrees that have disappeared since the last refresh.
        for stale in [k for k in self._cache.keys() if k and k not in live]:
            self._cache.pop(stale, None)

    def forget_worktree(self, worktree: str) -> None:
        """Drop a worktree's cache entry (used when the worktree is removed)."""
        self._cache.pop(worktree, None)

    def get_all_processes(self) -> list[dict]:
        """Flat, dedup-by-directory-name list of every known BP.

        Each entry:
            {
              "id":        process-id (from toml),
              "name":      directory name (filesystem-safe),
              "in_main":   bool — present in the main repo,
              "worktrees": list of worktree names where the same directory
                           name has a valid BP,
              "has_worktrees": derived (worktrees != []),
            }

        Worktree-only BPs surface as `in_main: false, worktrees: [<wt>]`.
        """
        # Build directory-name -> {in_main, worktrees, process_id} aggregations.
        by_name: Dict[str, dict] = {}
        for scope, processes in self._cache.items():
            for info in processes.values():
                entry = by_name.setdefault(
                    info.name,
                    {"id": info.id, "in_main": False, "worktrees": []},
                )
                if scope is None:
                    entry["in_main"] = True
                    # Main always wins as the canonical id source.
                    entry["id"] = info.id
                else:
                    entry["worktrees"].append(scope)

        out: list[dict] = []
        for name in sorted(by_name):
            entry = by_name[name]
            entry["worktrees"].sort()
            out.append(
                {
                    "id": entry["id"],
                    "name": name,
                    "in_main": entry["in_main"],
                    "worktrees": entry["worktrees"],
                    "has_worktrees": bool(entry["worktrees"]),
                }
            )
        return out

    def get_process_attachments(self, process_id: str) -> list[str]:
        """Get attachments for a specific process."""
        attachments = []

        process_dir = self._find_process_dir_by_id(process_id)
        if not process_dir:
            return attachments

        process_path = os.path.join(self.workspace_repo_dir, process_dir)
        if not process_path or not os.path.exists(process_path):
            return attachments

        attachments_dir = os.path.join(process_path, "Attachments")
        if not os.path.exists(attachments_dir):
            return attachments

        for item in os.listdir(attachments_dir):
            if os.path.isfile(os.path.join(attachments_dir, item)):
                attachments.append(item)

        return attachments

    def get_process_automation_sources(self, process_id: str) -> list[str]:
        """Get automation sources for a specific process."""
        automation_sources = []

        process_dir = self._find_process_dir_by_id(process_id)
        if not process_dir:
            return automation_sources

        process_path = os.path.join(self.workspace_repo_dir, process_dir)
        if not process_path or not os.path.exists(process_path):
            return automation_sources

        # Read bitswan.yaml to get deployment information
        bs_yaml = read_bitswan_yaml(self.gitops_dir)
        if not bs_yaml or "deployments" not in bs_yaml:
            return automation_sources

        # Look for subdirectories in the process folder
        for item in os.listdir(process_path):
            item_path = os.path.join(process_path, item)
            if os.path.isdir(item_path) and item != "Attachments":
                # This could be an automation source
                # Check if there's a deployment for this path
                deployment_id = self._find_deployment_for_path(
                    f"{process_dir}/{item}", bs_yaml
                )
                if deployment_id is not None:
                    automation_sources.append(deployment_id)

        return automation_sources

    def _find_process_dir_by_id(self, process_id: str) -> Optional[str]:
        """Find the directory name for a given process ID."""
        if not os.path.exists(self.workspace_repo_dir):
            return None

        for item in os.listdir(self.workspace_repo_dir):
            process_path = os.path.join(self.workspace_repo_dir, item)
            if not os.path.isdir(process_path):
                continue

            process_toml_path = os.path.join(process_path, "process.toml")
            if not os.path.exists(process_toml_path):
                continue

            try:
                with open(process_toml_path, "r") as f:
                    process_config = toml.load(f)
                    if process_config.get("process-id") == process_id:
                        return item
            except Exception:
                continue

        return None

    def _find_deployment_for_path(
        self, path: str, bs_yaml: Dict[str, Any]
    ) -> Optional[str]:
        """Find deployment ID for a given path."""

        for deployment_id, config in bs_yaml["deployments"].items():
            relative_path = config.get("relative_path") or ""
            if relative_path.endswith(path):
                return deployment_id

        return None

    def get_attachment_content(self, process_id: str, filename: str) -> Optional[bytes]:
        """Get content of a specific attachment."""
        process_dir = self._find_process_dir_by_id(process_id)
        if not process_dir:
            return None

        # Sanitize filename to prevent path traversal
        filename = os.path.basename(filename)

        attachment_path = os.path.join(
            self.workspace_repo_dir, process_dir, "Attachments", filename
        )

        if not os.path.exists(attachment_path):
            return None

        try:
            with open(attachment_path, "rb") as f:
                return f.read()
        except Exception:
            return None

    def set_attachment_content(
        self, process_id: str, filename: str, content: bytes
    ) -> bool:
        """Set content of a specific attachment."""
        process_dir = self._find_process_dir_by_id(process_id)
        if not process_dir:
            return False

        # Sanitize filename to prevent path traversal
        filename = os.path.basename(filename)

        attachments_dir = os.path.join(
            self.workspace_repo_dir, process_dir, "Attachments"
        )
        os.makedirs(attachments_dir, exist_ok=True)

        attachment_path = os.path.join(attachments_dir, filename)

        try:
            with open(attachment_path, "wb") as f:
                f.write(content)
            return True
        except Exception:
            return False

    def delete_attachment(self, process_id: str, filename: str) -> bool:
        """Delete a specific attachment."""
        process_dir = self._find_process_dir_by_id(process_id)
        if not process_dir:
            return False

        # Sanitize filename to prevent path traversal
        filename = os.path.basename(filename)

        attachment_path = os.path.join(
            self.workspace_repo_dir, process_dir, "Attachments", filename
        )

        try:
            if os.path.exists(attachment_path):
                os.remove(attachment_path)
                return True
        except Exception:
            pass
        return False

    def get_process_markdown(self, process_id: str) -> Optional[str]:
        """Get README.md content for a process."""
        process_dir = self._find_process_dir_by_id(process_id)
        if not process_dir:
            return None

        process_md_path = os.path.join(
            self.workspace_repo_dir, process_dir, "README.md"
        )

        if not os.path.exists(process_md_path):
            return None

        try:
            with open(process_md_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception:
            return None

    def set_process_markdown(self, process_id: str, content: str) -> bool:
        """Set README.md content for a process."""
        process_dir = self._find_process_dir_by_id(process_id)
        if not process_dir:
            return False

        process_md_path = os.path.join(
            self.workspace_repo_dir, process_dir, "README.md"
        )

        try:
            with open(process_md_path, "w", encoding="utf-8") as f:
                f.write(content)
            return True
        except Exception:
            return False

    def delete_process(self, process_id: str) -> bool:
        """Delete an entire process."""
        process_dir = self._find_process_dir_by_id(process_id)
        if not process_dir:
            return False

        process_path = os.path.join(self.workspace_repo_dir, process_dir)

        try:
            import shutil

            if os.path.exists(process_path):
                shutil.rmtree(process_path)
                return True
        except Exception:
            pass
        return False

    def create_process(self, process_id: str, process_name: str) -> bool:
        """Create a new process (MQTT-callable, main-repo only).

        Thin wrapper around `create_business_process` kept for backwards
        compatibility with the existing MQTT handler.
        """
        try:
            self.create_business_process(
                name=process_name, worktree=None, process_id=process_id
            )
            return True
        except Exception as e:
            logger.error(f"Error creating process {process_name}: {e}")
            return False

    def create_business_process(
        self,
        name: str,
        worktree: Optional[str] = None,
        process_id: Optional[str] = None,
    ) -> dict:
        """Create a new business-process directory with a `process.toml` +
        `README.md` template inside the main repo or a specific worktree.

        Returns the entry as it appears in `get_all_processes()`.
        """
        # Strip + basename to defend against path traversal. The HTTP route
        # additionally validates the input against a regex, but this keeps
        # the MQTT call path safe too.
        clean = os.path.basename((name or "").strip())
        if not clean:
            raise ValueError("process name is empty or invalid")

        if worktree:
            scope_root = os.path.join(self.workspace_repo_dir, "worktrees", worktree)
            if not os.path.isdir(scope_root):
                raise FileNotFoundError(f"worktree '{worktree}' does not exist")
        else:
            scope_root = self.workspace_repo_dir

        process_dir = os.path.join(scope_root, clean)
        if os.path.exists(process_dir):
            raise FileExistsError(
                f"a directory named '{clean}' already exists in "
                f"{'worktree ' + worktree if worktree else 'main'}"
            )

        pid = process_id or str(uuid.uuid4())

        os.makedirs(process_dir)
        with open(os.path.join(process_dir, "process.toml"), "w") as f:
            f.write(f'process-id = "{pid}"\n')
        with open(os.path.join(process_dir, "README.md"), "w") as f:
            f.write(f"# {clean}\n")

        # Refresh just the affected scope so the next discovery call sees
        # the new BP. The HTTP route is expected to broadcast the snapshot
        # over SSE after this returns; we keep the cache update local to
        # avoid coupling the service to the broadcaster.
        self.refresh(worktree)

        return {
            "id": pid,
            "name": clean,
            "in_main": worktree is None,
            "worktrees": [worktree] if worktree else [],
            "has_worktrees": bool(worktree),
        }


# Global process service instance
process_service = ProcessService()

# Store the event loop reference for running async functions from MQTT callbacks
_event_loop = None


def set_event_loop(loop: asyncio.AbstractEventLoop):
    """Set the asyncio event loop for running coroutines from MQTT callbacks."""
    global _event_loop
    _event_loop = loop


def run_coroutine_safe(coro: Coroutine):
    """Run a coroutine safely from a non-asyncio thread."""
    if _event_loop is None:
        logger.error("Event loop not set. Cannot run coroutine.")
        return

    future = asyncio.run_coroutine_threadsafe(coro, _event_loop)

    def log_exception(fut):
        """Log any exceptions from the coroutine."""
        try:
            # This will raise the exception if one occurred
            fut.result()
        except Exception as e:
            logger.error(f"Unhandled exception in coroutine: {e}", exc_info=True)

    # Add callback to log exceptions when the future completes
    future.add_done_callback(log_exception)


# MQTT topic patterns for process-related operations
TOPIC_PATTERNS = {
    # Process-level operations
    "process_gitops_req": re.compile(r"^/processes/c/([^/]+)/gitops-req$"),
    "process_set": re.compile(r"^/processes/c/([^/]+)/set$"),
    "process_create": re.compile(r"^/processes/c/([^/]+)/create$"),
    # Attachment operations
    "attachment_gitops_req": re.compile(
        r"^/processes/c/([^/]+)/attachments/c/([^/]+)/gitops-req$"
    ),
    "attachment_set": re.compile(r"^/processes/c/([^/]+)/attachments/c/([^/]+)/set$"),
}


def match_topic(topic: str) -> tuple[str, Match[str] | None]:
    """
    Match a topic against known patterns and return the pattern name and match object.

    Returns:
        tuple: (pattern_name, match_object) or ("unknown", None) if no match
    """
    for pattern_name, pattern in TOPIC_PATTERNS.items():
        match = pattern.match(topic)
        if match:
            return pattern_name, match
    return "unknown", None


async def publish_processes(client: mqtt_client.Client) -> ProcessList:
    """Publish the list of (main-repo) processes to MQTT.

    Driven by the cached main-repo entry — `WorkspaceChangeHandler` refreshes
    that entry before invoking us. Worktree processes are not (yet) published
    via this MQTT topic; they're surfaced over the new SSE `processes` event
    for the dashboard.
    """
    topic = "/processes/list"
    processes = process_service._cache.get(None)
    if processes is None:
        processes = process_service.refresh(None)

    process_list = ProcessList(processes=processes)

    client.publish(
        topic,
        payload=encode_pydantic_model(process_list),
        qos=1,
        retain=True,
    )

    return process_list


async def setup_mqtt_subscriptions(client: mqtt_client.Client):
    """Set up MQTT subscriptions for process-related topics."""

    # Store the current event loop
    set_event_loop(asyncio.get_event_loop())

    # Store the original on_message callback if it exists
    original_on_message = getattr(client, "on_message", None)

    def on_message(client, userdata, msg):
        topic = msg.topic

        # Handle process-related topics
        if topic.startswith("/processes/"):
            try:
                payload = json.loads(msg.payload.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                logger.error(f"Invalid JSON payload on topic {topic}")
                return

            # Match topic pattern and handle accordingly
            pattern_name, match = match_topic(topic)

            match pattern_name:
                case "process_gitops_req":
                    process_id = match.group(1)
                    run_coroutine_safe(
                        handle_process_request(client, process_id, payload)
                    )

                case "process_set":
                    process_id = match.group(1)
                    run_coroutine_safe(handle_process_set(client, process_id, payload))

                case "attachment_gitops_req":
                    process_id = match.group(1)
                    filename = match.group(2)
                    run_coroutine_safe(
                        handle_attachment_request(client, process_id, filename, payload)
                    )

                case "attachment_set":
                    process_id = match.group(1)
                    filename = match.group(2)
                    run_coroutine_safe(
                        handle_attachment_set(client, process_id, filename, payload)
                    )

                case "process_create":
                    process_id = match.group(1)
                    run_coroutine_safe(
                        handle_process_create(client, process_id, payload)
                    )

                case "unknown":
                    logger.error(f"Unknown process topic pattern: {topic}")

        else:
            # Call original callback for non-process topics
            if original_on_message:
                original_on_message(client, userdata, msg)

    client.on_message = on_message


async def handle_attachment_request(
    client: mqtt_client.Client, process_id: str, filename: str, payload: dict
):
    """Handle attachment get/delete requests."""
    action = payload.get("action")

    if action == "get":
        content = process_service.get_attachment_content(process_id, filename)
        if content is not None:
            content_topic = (
                f"/processes/c/{process_id}/attachments/c/{filename}/contents"
            )
            client.publish(content_topic, payload=content, qos=1)
    elif action == "delete":
        success = process_service.delete_attachment(process_id, filename)
        if not success:
            logger.error(
                f"Failed to delete attachment {filename} for process {process_id}"
            )


async def handle_attachment_set(
    client: mqtt_client.Client, process_id: str, filename: str, payload: dict
):
    """Handle attachment content setting."""
    if not payload.get("content"):
        logger.error(
            f"Failed to set attachment {filename} for process {process_id}: no content"
        )
        return

    content = base64.b64decode(payload.get("content"))
    success = process_service.set_attachment_content(process_id, filename, content)
    if not success:
        logger.error(f"Failed to set attachment {filename} for process {process_id}")


async def handle_process_request(
    client: mqtt_client.Client, process_id: str, payload: dict
):
    """Handle process get/delete requests."""
    action = payload.get("action")

    if action == "delete":
        success = process_service.delete_process(process_id)
        if not success:
            logger.error(f"Failed to delete process {process_id}")

    elif action == "get":
        content = process_service.get_process_markdown(process_id)
        if content is not None:
            content_topic = f"/processes/c/{process_id}/contents"
            client.publish(
                content_topic,
                payload=encode_pydantic_model(ProcessMarkdown(content=content)),
                qos=1,
            )
        else:
            logger.error(f"Failed to get process markdown for process {process_id}")
    else:
        logger.error(f"Unknown action {action} for process {process_id}")


async def handle_process_set(
    client: mqtt_client.Client, process_id: str, payload: dict
):
    """Handle process markdown setting."""
    content = payload.get("content", "")
    success = process_service.set_process_markdown(process_id, content)
    if not success:
        logger.error(
            f"Failed to set process markdown for process {process_id}: {content}"
        )


async def handle_process_create(
    client: mqtt_client.Client, process_id: str, payload: dict
):
    """Handle process creation."""
    process_name = payload.get("name")
    if not process_name:
        logger.error(f"Failed to create process {process_id}: process name is missing")
        return
    success = process_service.create_process(process_id, process_name)
    if not success:
        logger.error(f"Failed to create process {process_id}: {process_name}")
