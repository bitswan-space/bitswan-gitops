import asyncio
import hashlib
import logging
import os
import json
import re
import shutil
from tempfile import NamedTemporaryFile
import zipfile
import yaml
import requests
from datetime import datetime
from typing import Any, Callable
from app.models import DeployedAutomation
from app.utils import (
    add_workspace_route_to_ingress,
    AutomationConfig,
    get_expose_to_for_stage,
    calculate_git_tree_hash,
    docker_compose_up,
    generate_workspace_url,
    read_bitswan_yaml,
    read_pipeline_conf,
    read_automation_config,
    remove_route_from_ingress,
    update_git,
    call_git_command,
    call_git_command_with_output,
    copy_worktree,
    GitLockContext,
)
from app.async_docker import get_async_docker_client, DockerError
from app.services.image_service import ImageService
from app.services.oauth2_helpers import (
    OAUTH2_PROXY_PATH,
    copy_oauth2_proxy_to_container,
    is_oauth2_proxy_running,
)
from fastapi import UploadFile, HTTPException

logger = logging.getLogger(__name__)

# How often oauth2-proxy refreshes the session cookie (and access token).
# Should be shorter than the access token lifespan in Keycloak (15m for automation clients).
OAUTH2_COOKIE_REFRESH = "14m"


def _shorten_hostname_label(workspace_name: str, deployment_id: str) -> str:
    """Build a DNS hostname label, shortening the context if it would exceed 63 chars.

    DNS labels have a hard limit of 63 characters. When the full
    '{workspace}-{deployment_id}' label is too long, the middle part
    (BP name + worktree context) is replaced with a 4-char hash while
    keeping the automation name and stage visible.

    Example:
      Normal:  deployment-management-backend-dev  (32 chars, fine)
      Long:    deployment-management-backend-deployment-management-wt-coding-agent-staging-live-dev  (87 chars)
      Short:   deployment-management-backend-e8be-live-dev  (43 chars)
    """
    label = f"{workspace_name}-{deployment_id}"
    if len(label) <= 63:
        return label

    # deployment_id format: {automationName}-{context}
    # context format: {bp}-wt-{wt}-{stage} or {bp}-{stage}
    # Keep the automation name and stage, hash the middle (bp + worktree)
    parts = deployment_id.split("-")
    auto_name = parts[0]

    # Extract stage from the end (live-dev, dev, staging, or production-like)
    stage_suffix = ""
    if deployment_id.endswith("-live-dev"):
        stage_suffix = "-live-dev"
    elif deployment_id.endswith("-dev"):
        stage_suffix = "-dev"
    elif deployment_id.endswith("-staging"):
        stage_suffix = "-staging"

    # The middle part is everything between automation name and stage
    middle = deployment_id[len(auto_name) :]
    if stage_suffix:
        middle = middle[: -len(stage_suffix)]
    # middle is like "-deployment-management-wt-coding-agent-staging"

    short_hash = hashlib.sha256(middle.encode()).hexdigest()[:4]
    short_label = f"{workspace_name}-{auto_name}-{short_hash}{stage_suffix}"

    if len(short_label) > 63:
        # Still too long — truncate workspace name
        max_ws = 63 - len(f"-{auto_name}-{short_hash}{stage_suffix}")
        short_label = (
            f"{workspace_name[:max_ws]}-{auto_name}-{short_hash}{stage_suffix}"
        )

    return short_label


class AutomationService:
    def __init__(self):
        self.bs_home = os.environ.get("BITSWAN_GITOPS_DIR", "/mnt/repo/pipeline")
        self.bs_home_host = os.environ.get(
            "BITSWAN_GITOPS_DIR_HOST", "/home/root/.config/bitswan/local-gitops/"
        )
        self.workspace_id = os.environ.get("BITSWAN_WORKSPACE_ID")
        self.workspace_name = os.environ.get(
            "BITSWAN_WORKSPACE_NAME", "workspace-local"
        )
        self.aoc_url = os.environ.get("BITSWAN_AOC_URL")
        self.aoc_token = os.environ.get("BITSWAN_AOC_TOKEN")
        self.gitops_domain = os.environ.get("BITSWAN_GITOPS_DOMAIN")
        self.workspace_name = os.environ.get("BITSWAN_WORKSPACE_NAME")
        self.oauth2_proxy_path = OAUTH2_PROXY_PATH
        self.oauth2_proxy_port = 9999
        self.certs_dir_host = os.environ.get("BITSWAN_CERTS_DIR")
        self.gitops_dir = os.path.join(self.bs_home, "gitops")
        self.gitops_dir_host = os.path.join(self.bs_home_host, "gitops")
        self.secrets_dir = os.path.join(self.bs_home, "secrets")
        # Workspace directory for live-dev mode (source code mounting)
        # Uses same path structure as jupyter_service for consistency
        self.workspace_dir = os.path.join(self.bs_home, "workspace")
        self.workspace_dir_host = os.path.join(self.bs_home_host, "workspace")
        # Workspace repo directory (mounted at /workspace-repo in container)
        # Used to read automation.toml for live-dev config
        self.workspace_repo_dir = os.environ.get(
            "BITSWAN_WORKSPACE_REPO_DIR", "/workspace-repo"
        )
        # Cache full history per deployment_id: {deployment_id: (commit_hash, [entries])}
        self._history_cache: dict[str, tuple[str, list]] = {}

    async def warm_history_cache(self):
        """Pre-warm the history cache for all known deployments."""
        logger = logging.getLogger(__name__)
        bs_yaml = read_bitswan_yaml(self.gitops_dir)
        if not bs_yaml or "deployments" not in bs_yaml:
            logger.info("History cache warm-up: no deployments found, skipping")
            return

        deployment_ids = list(bs_yaml["deployments"].keys())
        logger.info(
            f"History cache warm-up: warming {len(deployment_ids)} deployment(s)"
        )
        for deployment_id in deployment_ids:
            try:
                await self.get_automation_history(deployment_id)
            except Exception as e:
                logger.warning(
                    f"History cache warm-up: failed for {deployment_id}: {e}"
                )
        logger.info("History cache warm-up: done")

    async def get_container(self, deployment_id) -> list[dict]:
        """Get containers for a specific deployment using async Docker client."""
        docker_client = get_async_docker_client()
        containers = await docker_client.list_containers(
            all=True,
            filters={
                "label": [
                    f"gitops.deployment_id={deployment_id}",
                    f"gitops.workspace={self.workspace_name}",
                ]
            },
        )
        return containers

    async def inspect_automation(self, deployment_id: str) -> list[dict]:
        """Get full docker inspect for all containers of a deployment."""
        containers = await self.get_container(deployment_id)
        if not containers:
            return []
        docker_client = get_async_docker_client()
        results = []
        for container in containers:
            container_id = container.get("Id") or container.get("id")
            if container_id:
                try:
                    inspect_data = await docker_client.get_container(container_id)
                    results.append(inspect_data)
                except Exception:
                    pass  # container may have been removed
        return results

    async def get_containers(self) -> list[dict]:
        """Get all gitops containers using async Docker client."""
        docker_client = get_async_docker_client()
        containers = await docker_client.list_containers(
            all=True,
            filters={
                "label": [
                    "gitops.deployment_id",
                    f"gitops.workspace={self.workspace_name}",
                ]
            },
        )
        return containers

    async def get_automations(self):
        """Get all automations with their container status using async Docker client."""
        bs_yaml = read_bitswan_yaml(self.gitops_dir)

        if not bs_yaml:
            return []

        pres = {
            deployment_id: DeployedAutomation(
                container_id=None,
                endpoint_name=None,
                created_at=None,
                name=deployment_id,
                state=None,
                status=None,
                deployment_id=deployment_id,
                active=bs_yaml["deployments"][deployment_id].get("active", False),
                automation_url=None,
                relative_path=bs_yaml["deployments"][deployment_id].get(
                    "relative_path", None
                ),
                stage=bs_yaml["deployments"][deployment_id].get("stage", "production"),
                version_hash=bs_yaml["deployments"][deployment_id].get(
                    "checksum", None
                ),
                replicas=bs_yaml["deployments"][deployment_id].get("replicas", 1),
            )
            for deployment_id in bs_yaml["deployments"]
        }

        docker_client = get_async_docker_client()
        info = await docker_client.info()
        containers = await self.get_containers()

        gitops_domain = os.environ.get("BITSWAN_GITOPS_DOMAIN", None)

        # updated pres with active containers
        for container in containers:
            labels = container.get("Labels", {})
            deployment_id = labels.get("gitops.deployment_id")
            if deployment_id and deployment_id in pres:
                label = labels.get("gitops.intended_exposed", "false")

                url = generate_workspace_url(
                    self.workspace_name, deployment_id, gitops_domain, True
                )

                if label != "true":
                    url = None

                # Parse created timestamp
                created_str = container.get("Created")
                created_at = None
                if created_str:
                    try:
                        # Docker API returns Unix timestamp
                        created_at = datetime.utcfromtimestamp(created_str)
                    except (ValueError, TypeError):
                        pass

                # Get container state
                state = container.get("State", "unknown")
                status = container.get("Status", "")

                pres[deployment_id] = DeployedAutomation(
                    container_id=container.get("Id"),
                    endpoint_name=info.get("Name"),
                    created_at=created_at,
                    name=deployment_id,
                    state=state,
                    status=status,
                    deployment_id=deployment_id,
                    active=pres[deployment_id].active,
                    automation_url=url,
                    relative_path=pres[deployment_id].relative_path,
                    stage=pres[deployment_id].stage,
                    version_hash=pres[deployment_id].version_hash,
                    replicas=pres[deployment_id].replicas,
                )

        return list(pres.values())

    async def _upload_and_commit_asset(
        self,
        file: UploadFile,
        commit_message: str | Callable[[str], str],
        checksum: str,
    ) -> dict:
        """
        Shared logic for uploading an asset (zip file), unpacking it,
        and committing it to git. Returns dict with checksum and output_directory.

        commit_message can be a string or a callable that takes checksum and returns a string.
        checksum: Pre-calculated git tree hash that will be verified.

        Uses async git lock to prevent race conditions with concurrent uploads.
        """
        with NamedTemporaryFile(delete=False) as temp_file:
            content = await file.read()
            temp_file.write(content)

            temp_file.close()
            output_dir = f"{checksum}"

            try:
                output_dir = os.path.join(self.gitops_dir, output_dir)

                os.makedirs(output_dir, exist_ok=True)
                with zipfile.ZipFile(temp_file.name, "r") as zip_ref:
                    zip_ref.extractall(output_dir)

                # Verify the checksum using git tree hash algorithm
                calculated_hash = await calculate_git_tree_hash(output_dir)
                if calculated_hash != checksum:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Checksum verification failed. Expected {checksum}, got {calculated_hash}",
                    )

                # Generate commit message if it's a callable
                if callable(commit_message):
                    final_commit_message = commit_message(checksum)
                else:
                    final_commit_message = commit_message

                # Use async lock for git operations to prevent race conditions
                async with GitLockContext(timeout=10.0):
                    # add this file to git
                    await call_git_command(
                        "git", "add", f"{checksum}", cwd=self.gitops_dir
                    )

                    # commit the changes
                    await call_git_command(
                        "git",
                        "commit",
                        "-m",
                        final_commit_message,
                        cwd=self.gitops_dir,
                    )

                    # push the changes
                    await call_git_command("git", "push", cwd=self.gitops_dir)

                return {
                    "checksum": checksum,
                    "output_directory": output_dir,
                }
            except Exception as e:
                raise Exception(f"Error processing file: {str(e)}")
            finally:
                os.unlink(temp_file.name)

    async def create_automation(
        self,
        deployment_id: str,
        file: UploadFile,
        relative_path: str = None,
        checksum: str = None,
    ):
        if not checksum:
            raise HTTPException(status_code=400, detail="Checksum is required")
        result = await self._upload_and_commit_asset(
            file, f"Add {deployment_id} to bitswan.yaml", checksum=checksum
        )
        checksum = result["checksum"]
        output_dir = result["output_directory"]

        try:
            bitswan_yaml_path = os.path.join(self.gitops_dir, "bitswan.yaml")

            # Update or create bitswan.yaml
            data = read_bitswan_yaml(self.gitops_dir)

            data = data or {"deployments": {}}
            deployments = data["deployments"]  # should never raise KeyError

            deployments[deployment_id] = deployments.get(deployment_id, {})
            deployments[deployment_id]["checksum"] = checksum
            deployments[deployment_id]["active"] = True

            if relative_path:
                deployments[deployment_id]["relative_path"] = relative_path

            data["deployments"] = deployments
            with open(bitswan_yaml_path, "w") as f:
                yaml.dump(data, f)

            await update_git(
                self.gitops_dir, self.gitops_dir_host, deployment_id, "create"
            )

            return {
                "message": "File processed successfully",
                "output_directory": output_dir,
                "checksum": checksum,
            }
        except Exception as e:
            return {"error": f"Error processing file: {str(e)}"}

    async def upload_asset(self, file: UploadFile, checksum: str):
        """
        Upload an asset (zip file), unpack it, and return the checksum.
        Similar to create_automation but without deployment_id.

        checksum: Pre-calculated git tree hash that will be verified.
        """
        try:
            result = await self._upload_and_commit_asset(
                file, lambda checksum: f"Add asset {checksum}", checksum=checksum
            )
            return {
                "message": "Asset uploaded successfully",
                "output_directory": result["output_directory"],
                "checksum": result["checksum"],
            }
        except Exception as e:
            return {"error": f"Error processing file: {str(e)}"}

    async def upload_asset_from_path(self, file_path: str, checksum: str):
        """
        Upload an asset from a file path (for streaming uploads).
        Similar to upload_asset but takes a file path instead of UploadFile.

        checksum: Pre-calculated git tree hash that will be verified.
        """
        output_dir = os.path.join(self.gitops_dir, checksum)
        os.makedirs(output_dir, exist_ok=True)

        try:
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                zip_ref.extractall(output_dir)
        except zipfile.BadZipFile as e:
            shutil.rmtree(output_dir, ignore_errors=True)
            raise HTTPException(status_code=400, detail=f"Invalid zip file: {e}")

        # Verify the checksum using git tree hash algorithm
        calculated_hash = await calculate_git_tree_hash(output_dir)
        if calculated_hash != checksum:
            shutil.rmtree(output_dir, ignore_errors=True)
            logger.error(
                "Checksum mismatch: expected=%s got=%s — extracted dir removed",
                checksum,
                calculated_hash,
            )
            raise HTTPException(
                status_code=400,
                detail=f"Checksum verification failed. Expected {checksum}, got {calculated_hash}",
            )

        # Use async lock for git operations to prevent race conditions
        async with GitLockContext(timeout=10.0):
            await call_git_command("git", "add", f"{checksum}", cwd=self.gitops_dir)
            await call_git_command(
                "git",
                "commit",
                "-m",
                f"Add asset {checksum}",
                cwd=self.gitops_dir,
            )
            await call_git_command("git", "push", cwd=self.gitops_dir)

        return {
            "message": "Asset uploaded successfully",
            "output_directory": output_dir,
            "checksum": checksum,
        }

    async def get_asset_diff(
        self, from_checksum: str, to_checksum: str, word_diff: bool = False
    ):
        """
        Compute a diff between two asset directories identified by checksum.
        Uses `git diff --no-index` which is read-only and requires no git lock.
        """
        import re

        # Validate checksums are hex strings of expected length
        hex_pattern = re.compile(r"^[0-9a-fA-F]{40}$|^[0-9a-fA-F]{64}$")
        if not hex_pattern.match(from_checksum):
            raise HTTPException(
                status_code=400,
                detail="Invalid from_checksum: must be a 40 or 64 character hex string",
            )
        if not hex_pattern.match(to_checksum):
            raise HTTPException(
                status_code=400,
                detail="Invalid to_checksum: must be a 40 or 64 character hex string",
            )

        # Early return if checksums are identical
        if from_checksum == to_checksum:
            return {
                "diff": "",
                "identical": True,
                "from_checksum": from_checksum,
                "to_checksum": to_checksum,
                "truncated": False,
            }

        # Determine paths based on HOST_PATH
        host_path = os.environ.get("HOST_PATH")
        if host_path:
            base_dir = self.gitops_dir_host
        else:
            base_dir = self.gitops_dir

        from_dir = os.path.join(base_dir, from_checksum)
        to_dir = os.path.join(base_dir, to_checksum)

        # Check directories exist (use local paths for existence check)
        from_dir_local = os.path.join(self.gitops_dir, from_checksum)
        to_dir_local = os.path.join(self.gitops_dir, to_checksum)

        if not os.path.isdir(from_dir_local):
            raise HTTPException(
                status_code=404,
                detail=f"Asset directory not found for checksum: {from_checksum}",
            )
        if not os.path.isdir(to_dir_local):
            raise HTTPException(
                status_code=404,
                detail=f"Asset directory not found for checksum: {to_checksum}",
            )

        # Build git diff command
        diff_args = ["git", "diff", "--no-index"]
        if word_diff:
            diff_args.append("--word-diff")
        diff_args.extend([from_dir, to_dir])

        stdout, stderr, return_code = await call_git_command_with_output(
            *diff_args, cwd=base_dir
        )

        # Exit codes: 0=identical, 1=diffs found, >1=error
        if return_code > 1:
            raise HTTPException(
                status_code=500,
                detail=f"Error computing diff: {stderr}",
            )

        identical = return_code == 0

        # Post-process: replace full directory paths with a/ and b/ prefixes
        diff_output = stdout
        diff_output = diff_output.replace(from_dir + "/", "a/")
        diff_output = diff_output.replace(to_dir + "/", "b/")
        diff_output = diff_output.replace(from_dir, "a")
        diff_output = diff_output.replace(to_dir, "b")

        # Truncate at 1MB
        max_size = 1 * 1024 * 1024
        truncated = len(diff_output) > max_size
        if truncated:
            diff_output = diff_output[:max_size]

        return {
            "diff": diff_output,
            "identical": identical,
            "from_checksum": from_checksum,
            "to_checksum": to_checksum,
            "truncated": truncated,
        }

    def download_asset(self, checksum: str) -> bytes:
        """
        Create a zip archive of the asset directory identified by checksum.
        Returns the zip bytes for streaming to the client.
        """
        import re
        import io

        hex_pattern = re.compile(r"^[0-9a-fA-F]{40}$|^[0-9a-fA-F]{64}$")
        if not hex_pattern.match(checksum):
            raise HTTPException(
                status_code=400,
                detail="Invalid checksum: must be a 40 or 64 character hex string",
            )

        asset_dir = os.path.join(self.gitops_dir, checksum)
        if not os.path.isdir(asset_dir):
            raise HTTPException(
                status_code=404,
                detail=f"Asset directory not found for checksum: {checksum}",
            )

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, _dirs, files in os.walk(asset_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, asset_dir)
                    zf.write(file_path, arcname)

        return buf.getvalue()

    def list_assets(self):
        """
        List all assets (checksum directories) in the gitops directory.
        """
        assets = []
        if not os.path.exists(self.gitops_dir):
            return assets

        for item in os.listdir(self.gitops_dir):
            item_path = os.path.join(self.gitops_dir, item)
            # Check if it's a directory and looks like a checksum (hex string, typically 40 chars for SHA1)
            if (
                os.path.isdir(item_path)
                and (len(item) == 40 or len(item) == 64)
                and all(c in "0123456789abcdef" for c in item.lower())
            ):
                assets.append(
                    {
                        "checksum": item,
                        "path": item_path,
                        "exists": os.path.exists(item_path),
                    }
                )
        return assets

    async def _get_latest_commit_hash(self, bitswan_dir: str) -> str:
        """
        Get the latest commit hash (HEAD) from the git repository.
        """
        stdout, stderr, return_code = await call_git_command_with_output(
            "git",
            "rev-parse",
            "HEAD",
            cwd=bitswan_dir,
        )
        if return_code != 0:
            raise HTTPException(
                status_code=500, detail=f"Error getting latest commit hash: {stderr}"
            )
        return stdout.strip()

    async def _fetch_yaml_at_commit(
        self, commit_hash: str, bitswan_dir: str
    ) -> tuple[str, str | None]:
        stdout, stderr, rc = await call_git_command_with_output(
            "git", "show", f"{commit_hash}:bitswan.yaml", cwd=bitswan_dir
        )
        if rc != 0:
            return commit_hash, None
        return commit_hash, stdout

    async def get_automation_history(
        self, deployment_id: str, page: int = 1, page_size: int = 20
    ):
        """
        Get paginated history of automation changes from git.
        Only includes entries where there are actual changes to the automation.
        Cached responses are invalidated when the commit hash changes.
        """

        host_path = os.environ.get("HOST_PATH")
        if host_path:
            bitswan_dir = self.gitops_dir_host
        else:
            bitswan_dir = self.gitops_dir

        # Get the latest commit hash
        current_commit_hash = await self._get_latest_commit_hash(bitswan_dir)

        # Check cache - we cache the full history per deployment, not per page
        if deployment_id in self._history_cache:
            cached_commit_hash, cached_entries = self._history_cache[deployment_id]
            if cached_commit_hash == current_commit_hash:
                return self._paginate_history(cached_entries, page, page_size)
            else:
                self._history_cache.clear()

        # Get commits that modified bitswan.yaml
        log_format = '{"commit": "%H", "author": "%an", "author_email": "%ae", "date": "%ai", "message": "%s"}'
        stdout, stderr, return_code = await call_git_command_with_output(
            "git",
            "log",
            "--format=" + log_format,
            "--date=iso",
            "--",
            "bitswan.yaml",
            cwd=bitswan_dir,
        )

        if return_code != 0:
            raise HTTPException(
                status_code=500, detail=f"Error getting git history: {stderr}"
            )

        commits = []
        for line in stdout.strip().split("\n"):
            if not line.strip():
                continue
            try:
                commit_data = json.loads(line)
                commits.append(commit_data)
            except json.JSONDecodeError:
                continue

        # Process commits in batches — stop early when we have enough entries.
        # Only cache the result if we processed ALL commits (complete history).
        # For each commit, compare this deployment's config with the parent commit
        # to determine if THIS commit actually modified the deployment.
        entries_needed = page * page_size
        BATCH_SIZE = 20

        history_entries = []
        processed_all = True

        for batch_start in range(0, len(commits), BATCH_SIZE):
            batch = commits[batch_start : batch_start + BATCH_SIZE]

            # Fetch bitswan.yaml for each commit AND its parent in parallel
            fetch_tasks = []
            for c in batch:
                fetch_tasks.append(self._fetch_yaml_at_commit(c["commit"], bitswan_dir))
                fetch_tasks.append(
                    self._fetch_yaml_at_commit(c["commit"] + "^", bitswan_dir)
                )
            results = await asyncio.gather(*fetch_tasks)
            content_by_key = dict(results)

            for commit in batch:
                commit_hash = commit["commit"]
                content = content_by_key.get(commit_hash)
                if content is None:
                    continue

                try:
                    commit_yaml = yaml.safe_load(content)
                    if not commit_yaml or "deployments" not in commit_yaml:
                        continue

                    deployment_config = commit_yaml.get("deployments", {}).get(
                        deployment_id
                    )

                    if deployment_config is None:
                        continue

                    current_checksum = deployment_config.get("checksum")
                    current_replicas = deployment_config.get("replicas", 1)

                    # Compare with parent commit to see if THIS commit
                    # actually changed this deployment
                    parent_content = content_by_key.get(commit_hash + "^")
                    parent_checksum = None
                    parent_replicas = None
                    if parent_content:
                        try:
                            parent_yaml = yaml.safe_load(parent_content)
                            if parent_yaml and "deployments" in parent_yaml:
                                parent_config = parent_yaml.get("deployments", {}).get(
                                    deployment_id
                                )
                                if parent_config:
                                    parent_checksum = parent_config.get("checksum")
                                    parent_replicas = parent_config.get("replicas", 1)
                        except yaml.YAMLError:
                            pass

                    checksum_changed = current_checksum != parent_checksum
                    replicas_changed = current_replicas != parent_replicas

                    if checksum_changed or replicas_changed:
                        entry = {
                            "commit": commit_hash,
                            "author": commit["author"],
                            "author_email": commit.get("author_email"),
                            "date": commit["date"],
                            "message": commit["message"],
                            "checksum": current_checksum,
                            "stage": deployment_config.get("stage", "production"),
                            "relative_path": deployment_config.get("relative_path"),
                            "active": deployment_config.get("active"),
                            "tag_checksum": deployment_config.get("tag_checksum"),
                            "replicas": current_replicas,
                        }
                        history_entries.append(entry)

                except yaml.YAMLError:
                    continue

            # Early exit: we have enough entries for the requested page
            if len(history_entries) >= entries_needed:
                processed_all = (batch_start + BATCH_SIZE) >= len(commits)
                break

        # Only cache if we processed all commits (complete history)
        if processed_all:
            self._history_cache[deployment_id] = (
                current_commit_hash,
                history_entries,
            )

        return self._paginate_history(history_entries, page, page_size)

    @staticmethod
    def _paginate_history(entries: list, page: int, page_size: int) -> dict:
        total = len(entries)
        start = (page - 1) * page_size
        end = start + page_size
        return {
            "items": entries[start:end],
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
        }

    async def start_oauth2_proxy_in_container(self, deployment_id: str):
        """Start oauth2-proxy in all running containers for a deployment"""
        containers = await self.get_container(deployment_id)

        if not containers:
            return False

        success = True
        for container in containers:
            container_id = container.get("Id")
            labels = container.get("Labels", {})
            container_name = container.get("Names", [deployment_id])[0].lstrip("/")

            # Check if oauth2 is enabled via labels
            if labels.get("gitops.oauth2.enabled") != "true":
                continue

            # Ensure container is running
            state = container.get("State", "")
            if state != "running":
                print(
                    f"Warning: Container {container_name} is not running (status: {state}), cannot start oauth2-proxy"
                )
                success = False
                continue

            try:
                docker_client = get_async_docker_client()

                # Check if oauth2-proxy is already running to avoid duplicates
                if await is_oauth2_proxy_running(docker_client, container_id):
                    print(f"oauth2-proxy already running in container {container_name}")
                    continue

                # Copy oauth2-proxy binary into the container
                if not await copy_oauth2_proxy_to_container(
                    container_id, container_name
                ):
                    success = False
                    continue

                # Build the backend logout URL from the host's OIDC issuer URL
                logout_flag = ""
                issuer_url = os.environ.get("OAUTH2_PROXY_OIDC_ISSUER_URL", "").strip()
                if issuer_url:
                    logout_url = f"{issuer_url}/protocol/openid-connect/logout?id_token_hint={{id_token}}"
                    logout_flag = f" --backend-logout-url='{logout_url}'"

                # Start oauth2-proxy in the background
                print(f"Starting oauth2-proxy in container {container_name}")
                cmd = [
                    "sh",
                    "-c",
                    f"oauth2-proxy{logout_flag} > /tmp/oauth2-proxy.log 2>&1 &",
                ]
                exec_id = await docker_client.exec_create(container_id, cmd)
                await docker_client.exec_start(exec_id)
                exec_info = await docker_client.exec_inspect(exec_id)

                if exec_info.get("ExitCode", 0) == 0:
                    print(
                        f"Successfully started oauth2-proxy in container {container_name}"
                    )
                else:
                    print(f"Failed to start oauth2-proxy in container {container_name}")
                    success = False

            except Exception as e:
                print(
                    f"Exception while starting oauth2-proxy in container {container_name}: {str(e)}"
                )
                success = False

        return success

    async def start_oauth2_proxy_in_infra_services(
        self, infra_service_names: list[str]
    ):
        """Start oauth2-proxy in infra service containers that have oauth2 labels.

        Uses the same docker exec pattern as start_oauth2_proxy_in_container
        but operates on infra service containers (e.g., pgAdmin) identified by
        their compose service names.
        """
        from app.async_docker import get_async_docker_client

        if not infra_service_names:
            return

        docker_client = get_async_docker_client()

        for svc_name in infra_service_names:
            try:
                containers = await docker_client.list_containers(
                    filters={"label": [f"com.docker.compose.service={svc_name}"]}
                )
                for container in containers:
                    container_id = container.get("Id")
                    labels = container.get("Labels", {})
                    container_name = container.get("Names", [svc_name])[0].lstrip("/")

                    if labels.get("gitops.oauth2.enabled") != "true":
                        continue

                    state = container.get("State", "")
                    if state != "running":
                        logger.warning(
                            f"Container {container_name} not running, "
                            f"cannot start oauth2-proxy"
                        )
                        continue

                    upstream_url = labels.get("gitops.oauth2.upstream")
                    if not upstream_url:
                        continue

                    # Check if already running
                    if await is_oauth2_proxy_running(docker_client, container_id):
                        logger.info(f"oauth2-proxy already running in {container_name}")
                        continue

                    # Copy oauth2-proxy binary into the container
                    if not await copy_oauth2_proxy_to_container(
                        container_id, container_name
                    ):
                        continue

                    logger.info(
                        f"Starting oauth2-proxy in {container_name} "
                        f"(upstream: {upstream_url})"
                    )
                    cmd = [
                        "sh",
                        "-c",
                        f"oauth2-proxy --upstream={upstream_url} "
                        f"> /tmp/oauth2-proxy.log 2>&1 &",
                    ]
                    exec_id = await docker_client.exec_create(container_id, cmd)
                    await docker_client.exec_start(exec_id)

                    exec_info = await docker_client.exec_inspect(exec_id)
                    if exec_info.get("ExitCode", 0) == 0:
                        logger.info(f"oauth2-proxy started in {container_name}")
                    else:
                        logger.error(
                            f"Failed to start oauth2-proxy in {container_name}"
                        )

            except Exception as e:
                logger.error(
                    f"Exception starting oauth2-proxy for infra service {svc_name}: {e}"
                )

    async def install_certificates_in_container(self, deployment_id: str):
        """Install CA certificates in all running containers for a deployment"""
        containers = await self.get_container(deployment_id)

        if not containers:
            return False

        success = True
        for container in containers:
            container_id = container.get("Id")
            labels = container.get("Labels", {})
            container_name = container.get("Names", [deployment_id])[0].lstrip("/")

            # Check if certificate installation is enabled via labels
            if labels.get("gitops.certs.enabled") != "true":
                continue

            # Ensure container is running
            state = container.get("State", "")
            if state != "running":
                print(
                    f"Warning: Container {container_name} is not running (status: {state}), cannot install certificates"
                )
                success = False
                continue

            try:
                docker_client = get_async_docker_client()

                # Install certificates: copy from custom dir, rename .pem to .crt, and update
                cert_install_script = """
if [ -d /usr/local/share/ca-certificates/custom ]; then
    cp /usr/local/share/ca-certificates/custom/*.crt /usr/local/share/ca-certificates/ 2>/dev/null || true
    cp /usr/local/share/ca-certificates/custom/*.pem /usr/local/share/ca-certificates/ 2>/dev/null || true
    for f in /usr/local/share/ca-certificates/*.pem; do
        [ -f "$f" ] && mv "$f" "${f%.pem}.crt"
    done
    update-ca-certificates 2>&1 | grep -v "WARNING" || true
    echo "CA certificates installed successfully"
else
    echo "No custom CA certificates directory found"
fi
"""
                print(f"Installing CA certificates in container {container_name}")
                cmd = ["sh", "-c", cert_install_script]
                exec_id = await docker_client.exec_create(container_id, cmd)
                output = await docker_client.exec_start(exec_id)
                exec_info = await docker_client.exec_inspect(exec_id)

                if exec_info.get("ExitCode", 0) == 0:
                    print(
                        f"Successfully installed certificates in container {container_name}: {output.strip()}"
                    )
                else:
                    print(
                        f"Failed to install certificates in container {container_name}: {output.strip()}"
                    )
                    success = False

            except Exception as e:
                print(
                    f"Exception while installing certificates in container {container_name}: {str(e)}"
                )
                success = False

        return success

    async def delete_automation(self, deployment_id: str):
        await self.remove_automation_from_bitswan(deployment_id)

        await update_git(self.gitops_dir, self.gitops_dir_host, deployment_id, "delete")
        result = remove_route_from_ingress(deployment_id, self.workspace_name)

        if not result:
            message = f"Deployment {deployment_id} deleted successfully, but failed to remove route from ingress"
        else:
            message = f"Deployment {deployment_id} deleted successfully"

        containers = await self.get_container(deployment_id)
        if containers:
            await self.remove_automation(deployment_id)
        return {"status": "success", "message": message}

    async def get_tag(self, deployed_image: str):
        """Get the sha tag for a deployed image using async Docker client."""
        expected_prefix = f"{deployed_image}:sha"
        try:
            docker_client = get_async_docker_client()
            image = await docker_client.get_image(deployed_image)
            tags = image.get("RepoTags", []) or []
            for tag in tags:
                if tag.startswith(expected_prefix):
                    deployed_image_checksum_tag = tag[len(expected_prefix) :]
                    return deployed_image_checksum_tag
            return None
        except DockerError:
            return None

    async def deploy_automation(
        self,
        deployment_id: str,
        checksum: str | None = None,
        stage: str | None = None,
        relative_path: str | None = None,
        # Automation config values (for live-dev, sent by extension)
        image: str | None = None,
        expose: bool | None = None,
        expose_to: list[str] | None = None,
        port: int | None = None,
        mount_path: str | None = None,
        secret_groups: list[str] | None = None,
        automation_id: str | None = None,
        auth: bool | None = None,
        allowed_domains: list[str] | None = None,
        services: dict | None = None,
        replicas: int | None = None,
        deployed_by: str | None = None,
        progress_callback: Callable[..., Any] | None = None,
    ):
        async def _report(step: str, message: str):
            if progress_callback is not None:
                await progress_callback(step, message)

        os.environ["COMPOSE_PROJECT_NAME"] = self.workspace_name
        bs_yaml = read_bitswan_yaml(self.gitops_dir)

        # Initialize bitswan.yaml if it doesn't exist
        if not bs_yaml:
            bs_yaml = {"deployments": {}}
            bitswan_yaml_path = os.path.join(self.gitops_dir, "bitswan.yaml")
            with open(bitswan_yaml_path, "w") as f:
                yaml.dump(bs_yaml, f)
            await update_git(
                self.gitops_dir,
                self.gitops_dir_host,
                deployment_id,
                "initialize",
                deployed_by=deployed_by,
            )

        await _report("updating_config", "Updating deployment configuration...")

        # Update bitswan.yaml with new parameters if provided
        has_updates = any(
            v is not None
            for v in [
                checksum,
                stage,
                relative_path,
                image,
                expose,
                expose_to,
                port,
                mount_path,
                secret_groups,
                automation_id,
                auth,
                allowed_domains,
                services,
                replicas,
            ]
        )
        if has_updates:
            if deployment_id not in bs_yaml.get("deployments", {}):
                bs_yaml.setdefault("deployments", {})[deployment_id] = {}

            deployment_config = bs_yaml["deployments"][deployment_id]

            if checksum is not None:
                deployment_config["checksum"] = checksum

            if stage is not None:
                # Map production to empty string
                deployment_config["stage"] = "" if stage == "production" else stage

            if relative_path is not None:
                deployment_config["relative_path"] = relative_path

            # Store automation config values (for live-dev deployments)
            if image is not None:
                deployment_config["image"] = image
            if expose is not None:
                deployment_config["expose"] = expose
            if expose_to is not None:
                deployment_config["expose_to"] = expose_to
            if port is not None:
                deployment_config["port"] = port
            if mount_path is not None:
                deployment_config["mount_path"] = mount_path
            if secret_groups is not None:
                deployment_config["secret_groups"] = secret_groups
            if automation_id is not None:
                deployment_config["id"] = automation_id
            if auth is not None:
                deployment_config["auth"] = auth
            if allowed_domains is not None:
                deployment_config["allowed_domains"] = allowed_domains
            if services is not None:
                deployment_config["services"] = services
            if replicas is not None:
                deployment_config["replicas"] = replicas

            # Set active to True by default when deploying (unless explicitly set to False)
            if "active" not in deployment_config:
                deployment_config["active"] = True

            bitswan_yaml_path = os.path.join(self.gitops_dir, "bitswan.yaml")
            with open(bitswan_yaml_path, "w") as f:
                yaml.dump(bs_yaml, f)

            await update_git(
                self.gitops_dir,
                self.gitops_dir_host,
                deployment_id,
                "deploy",
                deployed_by=deployed_by,
            )

            # Re-read to get updated config
            bs_yaml = read_bitswan_yaml(self.gitops_dir)

        # Auto-enable declared services for this deployment.
        # Check bitswan.yaml first, then fall back to automation config on disk
        # so that promoted deployments (which don't have services in bitswan.yaml)
        # still trigger service auto-enable.
        deployment_conf = bs_yaml.get("deployments", {}).get(deployment_id, {}) or {}
        deploy_services = services or deployment_conf.get("services")
        deploy_stage = stage or deployment_conf.get("stage") or "production"
        if deploy_stage == "":
            deploy_stage = "production"

        if not deploy_services and deploy_stage != "live-dev":
            # Read services from automation config on disk
            source = (
                deployment_conf.get("source")
                or deployment_conf.get("checksum")
                or deployment_id
            )
            source_dir = os.path.join(self.gitops_dir, source)
            if os.path.exists(source_dir):
                auto_conf = read_automation_config(source_dir)
                if auto_conf.services:
                    deploy_services = {
                        svc_name: {"enabled": svc_dep.enabled}
                        for svc_name, svc_dep in auto_conf.services.items()
                    }

        if deploy_services:
            await _report("enabling_services", "Enabling declared services...")
            await self.enable_services(deploy_services, deploy_stage)

        await _report(
            "generating_compose", "Generating docker-compose configuration..."
        )
        dc_yaml, infra_service_names = self.generate_docker_compose(bs_yaml)
        self._save_docker_compose(dc_yaml)
        deployments = bs_yaml.get("deployments", {})

        dc_config = yaml.safe_load(dc_yaml)

        # deploy the automation and its infra services
        await _report("docker_compose_up", "Starting containers...")
        deployment_result = await docker_compose_up(
            self.gitops_dir,
            dc_yaml,
            deployment_id,
            extra_services=infra_service_names,
        )
        await self._post_deploy_infra_services(bs_yaml)

        # record deployment in bitswan.yaml

        image_tag = None
        if deployment_id in dc_config.get("services", {}):
            deployed_image = dc_config["services"][deployment_id].get("image")
            image_tag = await self.get_tag(deployed_image)

        for result in deployment_result.values():
            if result["return_code"] != 0:
                raise HTTPException(
                    status_code=500,
                    detail=f"Error deploying services: \ndocker-compose:\n {dc_yaml}\n\nstdout:\n {result['stdout']}\nstderr:\n{result['stderr']}\n",
                )

        await _report("installing_certs", "Installing certificates...")
        await self.install_certificates_in_container(deployment_id)
        await _report("starting_oauth2_proxy", "Starting OAuth2 proxy...")
        await self.start_oauth2_proxy_in_container(deployment_id)
        await self.start_oauth2_proxy_in_infra_services(infra_service_names)

        if image_tag:
            await _report("storing_tags", "Recording image tag...")
            bs_yaml = read_bitswan_yaml(self.gitops_dir)
            if (
                bs_yaml
                and "deployments" in bs_yaml
                and deployment_id in bs_yaml["deployments"]
            ):
                bs_yaml["deployments"][deployment_id]["tag_checksum"] = image_tag

                bitswan_yaml_path = os.path.join(self.gitops_dir, "bitswan.yaml")
                with open(bitswan_yaml_path, "w") as f:
                    yaml.dump(bs_yaml, f)

                await update_git(
                    self.gitops_dir,
                    self.gitops_dir_host,
                    deployment_id,
                    "deploy",
                    deployed_by=deployed_by,
                )

        return {
            "message": "Deployed services successfully",
            "deployments": list(deployments.get(deployment_id, {}).keys()),
            "result": deployment_result,
        }

    async def deploy_automations(self):
        os.environ["COMPOSE_PROJECT_NAME"] = self.workspace_name
        bs_yaml = read_bitswan_yaml(self.gitops_dir)

        # Initialize bitswan.yaml if it doesn't exist
        if not bs_yaml:
            bs_yaml = {"deployments": {}}
            bitswan_yaml_path = os.path.join(self.gitops_dir, "bitswan.yaml")
            with open(bitswan_yaml_path, "w") as f:
                yaml.dump(bs_yaml, f)
            await update_git(self.gitops_dir, self.gitops_dir_host, "all", "initialize")

        active_deployments = self.get_active_automations()

        filtered_bs_yaml = {"deployments": active_deployments}

        # Auto-enable services for each active deployment.
        # Read from bitswan.yaml first, fall back to automation config on disk.
        for dep_id, dep_conf in active_deployments.items():
            if dep_conf is None:
                dep_conf = {}
                active_deployments[dep_id] = dep_conf
            dep_services = dep_conf.get("services")
            dep_stage = dep_conf.get("stage") or "production"
            if dep_stage == "":
                dep_stage = "production"

            if not dep_services and dep_stage != "live-dev":
                source = dep_conf.get("source") or dep_conf.get("checksum") or dep_id
                source_dir = os.path.join(self.gitops_dir, source)
                if os.path.exists(source_dir):
                    auto_conf = read_automation_config(source_dir)
                    if auto_conf.services:
                        dep_services = {
                            svc_name: {"enabled": svc_dep.enabled}
                            for svc_name, svc_dep in auto_conf.services.items()
                        }

            if dep_services:
                await self.enable_services(dep_services, dep_stage)

        dc_yaml, infra_names = self.generate_docker_compose(filtered_bs_yaml)
        self._save_docker_compose(dc_yaml)
        deployments = active_deployments

        # deploy_automations starts all services (no filter), so infra services
        # are included automatically via --remove-orphans
        deployment_result = await docker_compose_up(self.gitops_dir, dc_yaml)
        await self._post_deploy_infra_services(filtered_bs_yaml)

        for result in deployment_result.values():
            if result["return_code"] != 0:
                print(result["stdout"])
                print(result["stderr"])
                raise HTTPException(
                    status_code=500,
                    detail=f"Error deploying services: \nstdout:\n {result['stdout']}\nstderr:\n{result['stderr']}\n",
                )

        for deployment_id in deployments.keys():
            await self.install_certificates_in_container(deployment_id)
            await self.start_oauth2_proxy_in_container(deployment_id)
        await self.start_oauth2_proxy_in_infra_services(infra_names)

        return {
            "message": "Deployed services successfully",
            "deployments": list(deployments.keys()),
            "result": deployment_result,
        }

    async def scale_automation(self, deployment_id: str, replicas: int):
        """Scale an automation to the specified number of replicas."""
        os.environ["COMPOSE_PROJECT_NAME"] = self.workspace_name
        bs_yaml = read_bitswan_yaml(self.gitops_dir)

        if not bs_yaml or deployment_id not in bs_yaml.get("deployments", {}):
            raise HTTPException(
                status_code=404,
                detail=f"Deployment {deployment_id} not found",
            )

        deployment_config = bs_yaml["deployments"][deployment_id]
        stage = deployment_config.get("stage", "production")
        if stage == "":
            stage = "production"

        if stage == "live-dev":
            raise HTTPException(
                status_code=400,
                detail="Scaling is not supported for live-dev deployments",
            )

        deployment_config["replicas"] = replicas

        bitswan_yaml_path = os.path.join(self.gitops_dir, "bitswan.yaml")
        with open(bitswan_yaml_path, "w") as f:
            yaml.dump(bs_yaml, f)

        # Commit with a descriptive message using GitLockContext
        async with GitLockContext(timeout=10.0):
            await call_git_command("git", "add", "bitswan.yaml", cwd=self.gitops_dir)
            await call_git_command(
                "git",
                "commit",
                "-m",
                f"scale deployment {deployment_id} to {replicas} replicas",
                cwd=self.gitops_dir,
            )
            await call_git_command("git", "push", cwd=self.gitops_dir)

        # Ensure infrastructure services are enabled/running
        deploy_services = deployment_config.get("services")
        if not deploy_services:
            source = (
                deployment_config.get("source")
                or deployment_config.get("checksum")
                or deployment_id
            )
            source_dir = os.path.join(self.gitops_dir, source)
            if os.path.exists(source_dir):
                auto_conf = read_automation_config(source_dir)
                if auto_conf.services:
                    deploy_services = {
                        svc_name: {"enabled": svc_dep.enabled}
                        for svc_name, svc_dep in auto_conf.services.items()
                    }
        if deploy_services:
            await self.enable_services(deploy_services, stage)

        # Regenerate docker-compose and deploy
        bs_yaml = read_bitswan_yaml(self.gitops_dir)
        dc_yaml, infra_service_names = self.generate_docker_compose(bs_yaml)
        self._save_docker_compose(dc_yaml)

        deployment_result = await docker_compose_up(
            self.gitops_dir,
            dc_yaml,
            deployment_id,
            extra_services=infra_service_names,
        )
        await self._post_deploy_infra_services(bs_yaml)

        for result in deployment_result.values():
            if result["return_code"] != 0:
                raise HTTPException(
                    status_code=500,
                    detail=f"Error scaling deployment: \nstdout:\n {result['stdout']}\nstderr:\n{result['stderr']}\n",
                )

        # Run post-deploy hooks on all containers
        await self.install_certificates_in_container(deployment_id)
        await self.start_oauth2_proxy_in_container(deployment_id)
        await self.start_oauth2_proxy_in_infra_services(infra_service_names)

        return {
            "status": "success",
            "message": f"Scaled deployment {deployment_id} to {replicas} replicas",
            "replicas": replicas,
        }

    async def start_automation(self, deployment_id: str):
        """Start all containers for a deployment using async Docker client."""
        containers = await self.get_container(deployment_id)

        if not containers:
            raise HTTPException(
                status_code=404,
                detail=f"No container found for deployment ID: {deployment_id}",
            )

        docker_client = get_async_docker_client()
        for container in containers:
            container_id = container.get("Id")
            await docker_client.start_container(container_id)

        await self.install_certificates_in_container(deployment_id)
        await self.start_oauth2_proxy_in_container(deployment_id)

        return {
            "status": "success",
            "message": f"Container(s) for deployment {deployment_id} started successfully",
        }

    async def mark_as_inactive(self, deployment_id: str):
        """
        Mark the automation as inactive in bitswan.yaml
        and update git
        """
        bs_yaml = read_bitswan_yaml(self.gitops_dir)
        bs_yaml["deployments"][deployment_id]["active"] = False
        with open(os.path.join(self.gitops_dir, "bitswan.yaml"), "w") as f:
            yaml.dump(bs_yaml, f)
        await update_git(
            self.gitops_dir, self.gitops_dir_host, deployment_id, "mark_as_inactive"
        )

    async def mark_as_active(self, deployment_id: str):
        """
        Mark the automation as active in bitswan.yaml
        and update git
        """
        bs_yaml = read_bitswan_yaml(self.gitops_dir)
        bs_yaml["deployments"][deployment_id]["active"] = True
        with open(os.path.join(self.gitops_dir, "bitswan.yaml"), "w") as f:
            yaml.dump(bs_yaml, f)
        await update_git(
            self.gitops_dir, self.gitops_dir_host, deployment_id, "mark_as_active"
        )

    async def remove_automation_from_bitswan(self, deployment_id: str):
        """
        Remove the automation from bitswan.yaml
        and update git
        """
        bs_yaml = read_bitswan_yaml(self.gitops_dir)

        if deployment_id not in bs_yaml["deployments"]:
            return

        bs_yaml["deployments"].pop(deployment_id)
        with open(os.path.join(self.gitops_dir, "bitswan.yaml"), "w") as f:
            yaml.dump(bs_yaml, f)
        await update_git(self.gitops_dir, self.gitops_dir_host, deployment_id, "remove")

    # get active automations from bitswan.yaml
    def get_active_automations(self):
        """
        Get the active automations from bitswan.yaml
        """
        bs_yaml = read_bitswan_yaml(self.gitops_dir)
        active_deployments = {}
        for deployment_id, config in bs_yaml["deployments"].items():
            if config.get("active", False):
                active_deployments[deployment_id] = config
        return active_deployments

    async def stop_automation(self, deployment_id: str):
        """Stop all containers for a deployment using async Docker client."""
        containers = await self.get_container(deployment_id)

        if not containers:
            raise HTTPException(
                status_code=404,
                detail=f"No container found for deployment ID: {deployment_id}",
            )

        docker_client = get_async_docker_client()
        for container in containers:
            container_id = container.get("Id")
            await docker_client.stop_container(container_id)

        await self.mark_as_inactive(deployment_id)

        return {
            "status": "success",
            "message": f"Container(s) for deployment {deployment_id} stopped successfully",
        }

    async def restart_automation(self, deployment_id: str):
        """Restart all containers for a deployment using async Docker client."""
        containers = await self.get_container(deployment_id)

        if not containers:
            raise HTTPException(
                status_code=404,
                detail=f"No container found for deployment ID: {deployment_id}",
            )

        docker_client = get_async_docker_client()
        for container in containers:
            container_id = container.get("Id")
            await docker_client.restart_container(container_id)

        await self.install_certificates_in_container(deployment_id)
        await self.start_oauth2_proxy_in_container(deployment_id)

        return {
            "status": "success",
            "message": f"Container(s) for deployment {deployment_id} restarted successfully",
        }

    async def activate_automation(self, deployment_id: str):
        await self.mark_as_active(deployment_id)

        # update git
        await update_git(
            self.gitops_dir, self.gitops_dir_host, deployment_id, "activate"
        )

        result = await self.deploy_automation(deployment_id)

        return result

    async def deactivate_automation(self, deployment_id: str):
        await self.mark_as_inactive(deployment_id)

        # update git
        await update_git(
            self.gitops_dir, self.gitops_dir_host, deployment_id, "deactivate"
        )

        await self.remove_automation(deployment_id)

        return {
            "status": "success",
            "message": f"Deployment {deployment_id} deactivated successfully",
        }

    async def stream_automation_logs(
        self, deployment_id: str, lines: int = 200, since: int = 0
    ):
        """Stream container logs as SSE events (async generator).

        Yields SSE-formatted strings:
          event: metadata  — replica count, container info
          event: log       — {replica, line} for each log line
          event: error     — per-replica errors
          event: end       — all streams finished
          : keepalive      — periodic to keep connection alive
        """
        containers = await self.get_container(deployment_id)

        if not containers:
            yield f"event: error\ndata: {json.dumps({'message': 'No containers found'})}\n\n"
            yield "event: end\ndata: {}\n\n"
            return

        multiple = len(containers) > 1
        metadata = {
            "replicas": len(containers),
            "containers": [
                {
                    "id": c.get("Id", "")[:12],
                    "name": (c.get("Names") or ["unknown"])[0].lstrip("/"),
                    "state": c.get("State", "unknown"),
                }
                for c in containers
            ],
        }
        yield f"event: metadata\ndata: {json.dumps(metadata)}\n\n"

        docker_client = get_async_docker_client()
        queue: asyncio.Queue = asyncio.Queue()
        active_tasks = len(containers)

        async def read_replica(index: int, container_id: str):
            nonlocal active_tasks
            try:
                async for line in docker_client.stream_container_logs(
                    container_id, tail=lines, since=since
                ):
                    prefix = f"[replica-{index}] " if multiple else ""
                    await queue.put(
                        f"event: log\ndata: {json.dumps({'replica': index, 'line': prefix + line})}\n\n"
                    )
            except Exception as e:
                await queue.put(
                    f"event: error\ndata: {json.dumps({'replica': index, 'message': str(e)})}\n\n"
                )
            finally:
                active_tasks -= 1
                await queue.put(None)  # sentinel

        tasks = []
        for i, container in enumerate(containers):
            container_id = container.get("Id")
            tasks.append(asyncio.create_task(read_replica(i, container_id)))

        sentinels_received = 0
        keepalive_interval = 30
        while sentinels_received < len(containers):
            try:
                item = await asyncio.wait_for(queue.get(), timeout=keepalive_interval)
                if item is None:
                    sentinels_received += 1
                    continue
                yield item
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"

        yield "event: end\ndata: {}\n\n"

        for task in tasks:
            task.cancel()

    async def remove_automation(self, deployment_id: str):
        """Remove all containers for a deployment using async Docker client."""
        containers = await self.get_container(deployment_id)

        if not containers:
            raise HTTPException(
                status_code=404,
                detail=f"No container found for deployment ID: {deployment_id}",
            )

        docker_client = get_async_docker_client()
        for container in containers:
            container_id = container.get("Id")
            await docker_client.stop_container(container_id)
            await docker_client.remove_container(container_id)

        return {
            "status": "success",
            "message": f"Container(s) for deployment {deployment_id} removed successfully",
        }

    async def pull_and_deploy(self, branch_name: str):
        await copy_worktree(branch_name)

        bs_yaml = read_bitswan_yaml(self.gitops_dir)
        if not bs_yaml or "deployments" not in bs_yaml:
            raise HTTPException(
                status_code=404, detail="No deployments found in bitswan.yaml"
            )

        active_deployments = self.get_active_automations()
        image_tags = []

        for deployment_id, config in active_deployments.items():
            tag_checksum = config.get("tag_checksum")
            if not tag_checksum:
                continue

            images_root_dir = os.path.join(self.gitops_dir, "images", tag_checksum)
            source_dir = os.path.join(images_root_dir, "src")
            if not os.path.exists(source_dir):
                source_dir = images_root_dir

            if not os.path.exists(source_dir):
                continue

            image_service = ImageService()

            result = await image_service.create_image(
                image_tag=deployment_id,
                build_context_path=source_dir,
                checksum=tag_checksum,
            )
            image_tags.append(result["tag"])

        return {
            "status": "success",
            "message": f"Successfully synced branch {branch_name} and processed automations",
            "image_tags": image_tags,
        }

    def get_emqx_jwt_token(self, deployment_id: str):
        if not self.workspace_id:
            raise HTTPException(
                status_code=500,
                detail=f"Workspace {self.workspace_name} is missing an ID",
            )
        url = f"{self.aoc_url}/api/automation_server/workspaces/{self.workspace_id}/pipelines/{deployment_id}/emqx/jwt"
        headers = {"Authorization": f"Bearer {self.aoc_token}"}
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            error_detail = f"AOC API error: {response.status_code} - {response.text}"
            print(f"JWT Token generation failed: {error_detail}")
            return None
        return response.json()

    def add_keycloak_redirect_uri(self, redirect_uri: str):
        """Add a redirect URI to the workspace's Keycloak client"""
        if not self.workspace_id:
            print(
                f"Warning: Workspace {self.workspace_name} is missing an ID, skipping Keycloak redirect URI registration"
            )
            return None
        if not self.aoc_url or not self.aoc_token:
            print(
                "Warning: AOC URL or token not configured, skipping Keycloak redirect URI registration"
            )
            return None

        url = f"{self.aoc_url}/api/automation_server/workspaces/{self.workspace_id}/keycloak/add-redirect-uri/"

        headers = {
            "Authorization": f"Bearer {self.aoc_token}",
            "Content-Type": "application/json",
        }
        payload = {"redirect_uri": redirect_uri.strip()}

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            if response.status_code == 200:
                result = response.json()
                return result
            else:
                error_detail = (
                    f"Keycloak API error: {response.status_code} - {response.text}"
                )
                print(
                    f"Warning: Failed to add redirect URI to Keycloak: {error_detail}"
                )
                return None
        except Exception as e:
            print(f"Warning: Exception while adding redirect URI to Keycloak: {str(e)}")
            return None

    def get_or_create_automation_client(self, deployment_id, redirect_uri):
        """Get or create a Keycloak client for a specific automation (expose_to).

        Each automation gets its own client so it can have its own expose_to groups.
        Returns dict with client_id, client_secret, issuer_url or None.
        """
        if not self.workspace_id or not self.aoc_url or not self.aoc_token:
            print("Warning: AOC not configured, skipping automation client creation")
            return None

        url = f"{self.aoc_url}/api/automation_server/workspaces/{self.workspace_id}/keycloak/automation-client/"
        headers = {
            "Authorization": f"Bearer {self.aoc_token}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(
                url,
                headers=headers,
                json={"deployment_id": deployment_id, "redirect_uri": redirect_uri},
                timeout=30,
            )
            if response.status_code in (200, 201):
                return response.json()
            else:
                print(
                    f"Warning: Failed to get automation client: {response.status_code} - {response.text}"
                )
                return None
        except Exception as e:
            print(f"Warning: Exception getting automation client: {e}")
            return None

    def get_or_create_public_client(
        self,
        client_id: str,
        redirect_uri: str,
        web_origins: list[str] | None = None,
    ):
        """
        Get or create a public Keycloak client for frontend apps.

        Args:
            client_id: The client ID for the public client
            redirect_uri: The redirect URI for this deployment
            web_origins: List of allowed CORS origins for the client

        Returns:
            dict with client_id, issuer_url, etc. or None if failed
        """
        if not self.workspace_id:
            print(
                f"Warning: Workspace {self.workspace_name} is missing an ID, skipping public client creation"
            )
            return None
        if not self.aoc_url or not self.aoc_token:
            print(
                "Warning: AOC URL or token not configured, skipping public client creation"
            )
            return None

        url = f"{self.aoc_url}/api/automation_server/workspaces/{self.workspace_id}/keycloak/public-client/"

        headers = {
            "Authorization": f"Bearer {self.aoc_token}",
            "Content-Type": "application/json",
        }
        payload = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
        }

        if web_origins:
            payload["web_origins"] = web_origins

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            if response.status_code == 200:
                result = response.json()
                print(f"Successfully got/created public client: {client_id}")
                return result
            else:
                error_detail = (
                    f"Keycloak API error: {response.status_code} - {response.text}"
                )
                print(f"Warning: Failed to get/create public client: {error_detail}")
                return None
        except Exception as e:
            print(f"Warning: Exception while getting/creating public client: {str(e)}")
            return None

    async def _post_deploy_infra_services(self, bs_yaml: dict) -> None:
        """Call post-start init hooks for infra services after docker-compose up."""
        from app.services.infra_service import get_service, stage_for_deployment

        seen: set[tuple[str, str]] = set()
        for dep_conf in bs_yaml.get("deployments", {}).values():
            dep_conf = dep_conf or {}
            dep_stage = dep_conf.get("stage") or "production"
            mapped_stage = stage_for_deployment(dep_stage)
            for svc_type, svc_conf in (dep_conf.get("services") or {}).items():
                enabled = (
                    svc_conf.get("enabled", True)
                    if isinstance(svc_conf, dict)
                    else bool(svc_conf)
                )
                if not enabled or (svc_type, mapped_stage) in seen:
                    continue
                seen.add((svc_type, mapped_stage))
                try:
                    svc = get_service(svc_type, self.workspace_name, stage=mapped_stage)
                except ValueError:
                    continue
                if hasattr(svc, "initialize"):
                    try:
                        await svc.initialize()
                    except Exception as e:
                        logger.warning(
                            f"Post-deploy init for {svc.display_name} failed: {e}"
                        )

    def get_org_group_path(self):
        """Fetch the Keycloak org group path for this workspace from AOC.

        Returns the group path string (e.g. "/Example Org"), or None if AOC
        is not configured (non-AOC deployments skip group-path resolution).
        Raises HTTPException if AOC is configured but the call fails.
        """
        if not self.workspace_id or not self.aoc_url or not self.aoc_token:
            return None

        url = f"{self.aoc_url}/api/automation_server/workspaces/{self.workspace_id}/keycloak/org-group-path/"
        headers = {"Authorization": f"Bearer {self.aoc_token}"}

        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                group_path = response.json().get("group_path")
                if not group_path:
                    raise HTTPException(
                        status_code=500,
                        detail="AOC returned empty org group path",
                    )
                print(f"Got org group path: {group_path}")
                return group_path
            else:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to get org group path: {response.status_code} - {response.text}",
                )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Exception fetching org group path: {e}",
            )

    async def enable_services(self, services: dict, stage: str) -> None:
        """Auto-enable infrastructure services for a specific deployment.

        Takes the services dict (e.g. {"kafka": {"enabled": true}}) and the
        deployment stage, and enables any declared services that aren't already
        running.
        """
        from app.services.infra_service import get_service, stage_for_deployment

        mapped_stage = stage_for_deployment(stage)

        for svc_type, svc_conf in services.items():
            enabled = (
                svc_conf.get("enabled", True)
                if isinstance(svc_conf, dict)
                else bool(svc_conf)
            )
            if not enabled:
                continue

            try:
                svc = get_service(svc_type, self.workspace_name, stage=mapped_stage)
            except ValueError:
                logger.warning(
                    f"Unknown service type '{svc_type}', skipping auto-enable"
                )
                continue

            if not svc.is_enabled():
                logger.info(
                    f"Auto-enabling {svc.display_name} for workspace '{self.workspace_name}'"
                )
                try:
                    await svc.enable()
                except Exception as e:
                    logger.error(f"Failed to auto-enable {svc.display_name}: {e}")
            else:
                running = await svc.is_running()
                if not running:
                    logger.warning(
                        f"{svc.display_name} is enabled but not running — "
                        f"attempting to start container"
                    )
                    try:
                        await svc.start()
                        logger.info(f"{svc.display_name} started successfully")
                    except Exception:
                        # Container may have been removed entirely;
                        # docker-compose up will recreate it.
                        logger.info(
                            f"Could not start {svc.display_name} container "
                            f"(may have been removed) — docker-compose up will recreate it"
                        )
                        # Re-register with ingress in case route was lost
                        try:
                            await svc._register_with_caddy()
                        except Exception as e:
                            logger.warning(
                                f"Failed to re-register {svc.display_name} with ingress: {e}"
                            )

    def _resolve_service_secrets(
        self, automation_config: AutomationConfig, stage: str
    ) -> list[str]:
        """Resolve service dependencies and return list of secret file names to inject.

        Uses the deployment's own stage to determine the service realm
        (live-dev maps to dev). Returns the corresponding secrets file names
        (e.g., 'kafka-dev', 'couchdb-production').
        """
        if not automation_config.services:
            return []

        from app.services.infra_service import get_service, stage_for_deployment

        mapped_stage = stage_for_deployment(stage)

        secret_names = []
        for svc_type, svc_dep in automation_config.services.items():
            if not svc_dep.enabled:
                continue

            try:
                svc = get_service(svc_type, self.workspace_name, stage=mapped_stage)
            except ValueError:
                logger.warning(f"Unknown service type '{svc_type}', skipping")
                continue

            secret_names.append(svc.secrets_file_name)
            logger.info(
                f"Service dependency: {svc.display_name} -> secrets '{svc.secrets_file_name}'"
            )

        return secret_names

    def generate_docker_compose(self, bs_yaml: dict):
        dc = {
            "version": "3",
            "services": {},
        }
        external_networks = {"bitswan_network"}
        deployments = bs_yaml.get("deployments", {})
        for deployment_id, conf in deployments.items():
            if conf is None:
                conf = {}
                deployments[deployment_id] = conf
            entry = {}

            source = conf.get("source") or conf.get("checksum") or deployment_id
            source_dir = os.path.join(self.gitops_dir, source)

            # For live-dev with relative_path, use workspace directory for config
            stage = conf.get("stage", "production")
            if stage == "":
                stage = "production"
            relative_path = conf.get("relative_path")

            if stage == "live-dev" and relative_path:
                # For live-dev, read automation.toml directly from the workspace
                live_dev_source_dir = os.path.join(
                    self.workspace_repo_dir, relative_path
                )
                if not os.path.isdir(live_dev_source_dir):
                    continue
                automation_config = read_automation_config(live_dev_source_dir)
                if not automation_config.image:
                    continue
                pipeline_conf = None
            elif not os.path.exists(source_dir):
                raise HTTPException(
                    status_code=500,
                    detail=f"Deployment directory {source_dir} does not exist",
                )
            else:
                pipeline_conf = read_pipeline_conf(source_dir)
                automation_config = read_automation_config(source_dir)

            # Ensure services from automation config on disk are reflected in
            # the deployment conf so _merge_infra_services() can discover them.
            # Without this, promoted deployments (dev/staging/production) whose
            # bitswan.yaml entry lacks a "services" key would be invisible to
            # the infra-service merge step, and their Kafka/CouchDB/etc.
            # containers could be removed as orphans.
            if automation_config.services and not conf.get("services"):
                conf["services"] = {
                    svc_name: {"enabled": svc_dep.enabled}
                    for svc_name, svc_dep in automation_config.services.items()
                }

            if self.workspace_id and self.aoc_url and self.aoc_token:
                # generate jwt token for automation
                # jwt_token_response = self.get_emqx_jwt_token(deployment_id)
                # if jwt_token_response is not None:
                #     jwt_token = jwt_token_response.get("token")
                #     emqx_url = jwt_token_response.get("url")
                #     entry["environment"] = {
                #         "MQTT_USERNAME": deployment_id,
                #         "MQTT_PASSWORD": jwt_token,
                #         "MQTT_BROKER_URL": emqx_url,
                #         "DEPLOYMENT_ID": deployment_id,
                #     }
                print("Skipping JWT token generation")
            else:
                entry["environment"] = {"DEPLOYMENT_ID": deployment_id}
            replicas = conf.get("replicas", 1)
            if replicas <= 1:
                entry["container_name"] = f"{self.workspace_name}__{deployment_id}"
            entry["restart"] = "always"
            entry["ulimits"] = {"nofile": {"soft": 65536, "hard": 65536}}
            entry["labels"] = {
                "gitops.deployment_id": deployment_id,
                "gitops.workspace": self.workspace_name,
                "gitops.intended_exposed": "false",
            }
            entry["image"] = "bitswan/pipeline-runtime-environment:latest"

            # Set BITSWAN environment variables (stage already determined above)
            if "environment" not in entry:
                entry["environment"] = {}
            entry["environment"]["BITSWAN_AUTOMATION_STAGE"] = stage
            entry["environment"]["BITSWAN_DEPLOYMENT_ID"] = deployment_id
            if self.workspace_name:
                entry["environment"]["BITSWAN_WORKSPACE_NAME"] = self.workspace_name
            if self.gitops_domain:
                entry["environment"]["BITSWAN_GITOPS_DOMAIN"] = self.gitops_domain
            # Deployment context for service discovery.
            # Context = {bp}-wt-{wt}-{stage} or {bp}-{stage} or {bp} (production)
            # URL template lets automations find each other by substituting {name}.
            # Deployment ID format: {automationName}-{context}
            deployment_context = conf.get("deployment_context", "")
            if not deployment_context:
                # Derive context from relative_path and stage
                # relative_path is like "Test/backend" or "worktrees/bar/Test/backend"
                bp_name = ""
                wt_name = ""
                if relative_path:
                    parts = relative_path.replace("\\", "/").split("/")
                    if len(parts) >= 2 and parts[0] == "worktrees":
                        wt_name = parts[1]  # extract worktree name
                        parts = parts[2:]  # skip "worktrees/{name}"
                    if len(parts) >= 2:
                        bp_name = parts[0]
                bp_sanitized = (
                    re.sub(r"[^a-z0-9-]", "-", bp_name.lower()).strip("-")
                    if bp_name
                    else ""
                )
                wt_part = f"-wt-{wt_name}" if wt_name else ""
                stage_suffix = f"-{stage}" if stage and stage != "production" else ""
                if bp_sanitized:
                    deployment_context = f"{bp_sanitized}{wt_part}{stage_suffix}"
                elif wt_name:
                    deployment_context = f"wt-{wt_name}{stage_suffix}"
                else:
                    deployment_context = (
                        stage if stage and stage != "production" else ""
                    )

            if deployment_context:
                entry["environment"]["BITSWAN_DEPLOYMENT_CONTEXT"] = deployment_context

            # For worktree live-devs, override POSTGRES_DB to use the cloned database
            if wt_name and stage == "live-dev":
                wt_db = "postgres_wt_" + re.sub(r"[^a-z0-9_]", "_", wt_name.lower())
                entry["environment"]["POSTGRES_DB"] = wt_db

            if self.workspace_name and self.gitops_domain:
                ctx_suffix = f"-{deployment_context}" if deployment_context else ""
                entry["environment"]["BITSWAN_URL_TEMPLATE"] = (
                    f"https://{self.workspace_name}-"
                    "{name}"
                    f"{ctx_suffix}.{self.gitops_domain}"
                )

            # Deployment and image checksums + deploy timestamp
            deploy_checksum = conf.get("checksum")
            if deploy_checksum:
                entry["environment"]["BITSWAN_DEPLOY_CHECKSUM"] = deploy_checksum
            image_checksum = conf.get("tag_checksum")
            if image_checksum:
                entry["environment"]["BITSWAN_IMAGE_CHECKSUM"] = image_checksum
            entry["environment"]["BITSWAN_DEPLOY_TIME"] = (
                datetime.utcnow().isoformat() + "Z"
            )

            network_mode = None
            secret_groups = []

            # Get secret groups based on config format
            if automation_config.config_format == "toml":
                # For TOML format, use stage-specific secrets only (no fallback)
                if stage == "live-dev":
                    # live-dev and dev share secrets — fall back to each other
                    secret_groups = (
                        automation_config.live_dev_groups
                        or automation_config.dev_groups
                        or []
                    )
                elif stage == "dev":
                    # dev and live-dev share secrets — fall back to each other
                    secret_groups = (
                        automation_config.dev_groups
                        or automation_config.live_dev_groups
                        or []
                    )
                elif stage == "staging" and automation_config.staging_groups:
                    secret_groups = automation_config.staging_groups
                elif stage == "production" and automation_config.production_groups:
                    secret_groups = automation_config.production_groups
            elif pipeline_conf:
                # For INI format (pipelines.conf), use existing logic with fallback
                network_mode = pipeline_conf.get(
                    "docker.compose", "network_mode", fallback=conf.get("network_mode")
                )

                # Check for stage-specific secret groups first, then fall back to groups
                stage_groups_key = f"{stage}_groups"
                secret_groups_str = pipeline_conf.get(
                    "secrets", stage_groups_key, fallback=""
                )
                if not secret_groups_str:
                    # Fall back to generic groups if stage-specific groups not set
                    secret_groups_str = pipeline_conf.get(
                        "secrets", "groups", fallback=""
                    )
                secret_groups = (
                    secret_groups_str.split(" ") if secret_groups_str else []
                )

            for secret_group in secret_groups:
                # Skip empty secret groups
                if not secret_group:
                    continue
                if os.path.exists(self.secrets_dir):
                    secret_env_file = os.path.join(self.secrets_dir, secret_group)
                    if os.path.exists(secret_env_file):
                        if not entry.get("env_file"):
                            entry["env_file"] = []
                        entry["env_file"].append(secret_env_file)

            # Inject service dependency secrets (from [services.*] in automation.toml)
            service_secret_names = self._resolve_service_secrets(
                automation_config, stage
            )
            for svc_secret_name in service_secret_names:
                svc_secret_path = os.path.join(self.secrets_dir, svc_secret_name)
                if os.path.exists(svc_secret_path):
                    if not entry.get("env_file"):
                        entry["env_file"] = []
                    if svc_secret_path not in entry["env_file"]:
                        entry["env_file"].append(svc_secret_path)

            if not network_mode:
                network_mode = conf.get("network_mode")

            # external-testing-network: isolated bridge with only outbound internet.
            # No access to internal services — tests must use public URLs.
            if not network_mode and automation_config.external_testing_network:
                networks_list = ["bitswan_external_testing"]
                external_networks.add("bitswan_external_testing")

            if network_mode:
                entry["network_mode"] = network_mode
            elif not automation_config.external_testing_network:
                if "networks" in conf:
                    networks_list = conf["networks"].copy()
                elif "default-networks" in bs_yaml:
                    networks_list = bs_yaml["default-networks"].copy()
                else:
                    networks_list = ["bitswan_network"]

            if not network_mode:
                if replicas > 1:
                    # Use network aliases instead of container_name for DNS round-robin
                    alias = f"{self.workspace_name}__{deployment_id}"
                    entry["networks"] = {
                        net: {"aliases": [alias]} for net in networks_list
                    }
                    entry["deploy"] = {"replicas": replicas}
                else:
                    entry["networks"] = networks_list

            if entry.get("networks"):
                if isinstance(entry["networks"], dict):
                    external_networks.update(entry["networks"].keys())
                else:
                    external_networks.update(set(entry["networks"]))

            passthroughs = ["volumes", "ports", "devices"]
            if replicas <= 1:
                passthroughs.append("container_name")
            entry.update({p: conf[p] for p in passthroughs if p in conf})

            deployment_dir = os.path.join(self.gitops_dir_host, source)

            # Use unified automation config for image, expose, and port
            entry["image"] = automation_config.image
            expose = automation_config.expose
            port = automation_config.port
            expose_to_groups = get_expose_to_for_stage(automation_config, stage)

            # Resolve group paths if expose_to_groups is set and AOC is configured.
            # Without AOC, group-based exposure is silently skipped (simple-mode deploy).
            if expose_to_groups:
                org_group_path = self.get_org_group_path()
                if org_group_path:
                    resolved = []
                    for g in expose_to_groups:
                        if g == "*":
                            resolved.append(org_group_path)
                        else:
                            resolved.append(f"{org_group_path}{g}")
                    expose_to_groups = resolved
                else:
                    expose_to_groups = []

            # Error if both expose and expose_to_groups are set
            if expose and expose_to_groups:
                raise HTTPException(
                    status_code=400,
                    detail="Cannot specify both 'expose' and 'expose_to'. Use 'expose_to' alone to enable exposure with OAuth2.",
                )

            # If expose_to_groups is set, automatically enable exposure
            if expose_to_groups:
                expose = True

            if expose and port:
                # Set URL env vars for exposed automations
                # Shorten hostname if it would exceed DNS 63-char label limit
                url_label = _shorten_hostname_label(self.workspace_name, deployment_id)
                url_prefix = f"https://{self.workspace_name}-"
                url_suffix = f".{self.gitops_domain}"
                automation_url = f"https://{url_label}.{self.gitops_domain}"

                entry["environment"]["BITSWAN_AUTOMATION_URL"] = automation_url
                entry["environment"]["BITSWAN_URL_PREFIX"] = url_prefix
                entry["environment"]["BITSWAN_URL_SUFFIX"] = url_suffix

                if expose_to_groups:
                    endpoint = automation_url
                    redirect_uri = f"{endpoint}/oauth2/callback"

                    # Get or create a dedicated automation client
                    # (one per automation, so each can have its own expose_to)
                    automation_client = self.get_or_create_automation_client(
                        deployment_id, redirect_uri
                    )
                    if not automation_client:
                        raise Exception(
                            f"Failed to get or create automation client for expose_to on {deployment_id}"
                        )

                    # Start with the editor's OAUTH2 env vars as defaults,
                    # then override with per-automation specifics
                    oauth2_envs = {
                        k: v for k, v in os.environ.items() if k.startswith("OAUTH2")
                    }
                    oauth2_envs.update(
                        {
                            "OAUTH_ENABLED": "true",
                            "OAUTH2_PROXY_CLIENT_ID": automation_client["client_id"],
                            "OAUTH2_PROXY_CLIENT_SECRET": automation_client[
                                "client_secret"
                            ],
                            "OAUTH2_PROXY_SCOPE": "openid email profile",
                            "OAUTH2_PROXY_UPSTREAMS": f"http://127.0.0.1:{port}",
                            "OAUTH2_PROXY_REDIRECT_URL": redirect_uri,
                            "OAUTH2_PROXY_ALLOWED_GROUPS": ",".join(expose_to_groups),
                            "OAUTH2_PROXY_SET_XAUTHREQUEST": "true",
                            "OAUTH2_PROXY_PASS_ACCESS_TOKEN": "true",
                            "OAUTH2_PROXY_COOKIE_REFRESH": OAUTH2_COOKIE_REFRESH,
                            "BITSWAN_AUTOMATION_URL": automation_url,
                        }
                    )
                    entry["environment"].update(oauth2_envs)

                    # Store oauth2 config in labels for post-deployment execution
                    entry["labels"]["gitops.oauth2.enabled"] = "true"
                    entry["labels"]["gitops.intended_exposed"] = "true"
                    if not add_workspace_route_to_ingress(deployment_id, self.oauth2_proxy_port):
                        logger.warning(
                            f"Failed to add ingress route for {deployment_id} (oauth2 proxy port)"
                        )

                else:
                    entry["labels"]["gitops.intended_exposed"] = "true"
                    if not add_workspace_route_to_ingress(deployment_id, port):
                        logger.warning(
                            f"Failed to add ingress route for {deployment_id} — "
                            "deployment will proceed but you suck may not be externally reachable"
                        )

            # Add the public hostname as a network alias so other containers
            # on the same Docker network can reach this automation by its URL.
            if expose and port and self.gitops_domain and not network_mode:
                url_host = f"{_shorten_hostname_label(self.workspace_name, deployment_id)}.{self.gitops_domain}"
                networks = entry.get("networks")
                if isinstance(networks, dict):
                    for net_conf in networks.values():
                        aliases = net_conf.setdefault("aliases", [])
                        if url_host not in aliases:
                            aliases.append(url_host)
                elif isinstance(networks, list):
                    # Convert list form to dict form so we can attach aliases
                    entry["networks"] = {
                        net: {"aliases": [url_host]} for net in networks
                    }

            # Always pass Keycloak URL for JWT verification
            # KEYCLOAK_URL format: https://keycloak.example.com/realms/realm-name
            keycloak_url = os.environ.get("KEYCLOAK_URL", "")
            if keycloak_url:
                entry["environment"]["KEYCLOAK_URL"] = (
                    keycloak_url.rsplit("/realms/", 1)[0]
                    if "/realms/" in keycloak_url
                    else keycloak_url
                )
                entry["environment"]["KEYCLOAK_REALM"] = (
                    keycloak_url.rsplit("/realms/", 1)[-1]
                    if "/realms/" in keycloak_url
                    else ""
                )
                entry["environment"]["KEYCLOAK_ISSUER_URL"] = keycloak_url

            # Inject the org group path for JWT group-membership verification,
            # but only when AOC is configured (simple-mode deployments skip this).
            org_group_path = self.get_org_group_path()
            if org_group_path:
                entry["environment"]["BITSWAN_ALLOWED_GROUP"] = org_group_path

            if "volumes" not in entry:
                entry["volumes"] = []

            # Mount CA certificates if configured
            if self.certs_dir_host:
                entry["volumes"].append(
                    f"{self.certs_dir_host}:/usr/local/share/ca-certificates/custom:ro"
                )
                entry["environment"]["UPDATE_CA_CERTIFICATES"] = "true"
                entry["labels"]["gitops.certs.enabled"] = "true"

            # For live-dev stage, mount source from workspace (for live editing)
            # Otherwise, mount from gitops deployment directory (checksum dir)
            if stage == "live-dev" and relative_path:
                # Mount the original source code for live development
                # Uses same workspace path as jupyter_service
                source_mount_path = os.path.join(self.workspace_dir_host, relative_path)
                entry["volumes"].append(
                    f"{source_mount_path}:{automation_config.mount_path}"
                )
            else:
                entry["volumes"].append(
                    f"{deployment_dir}:{automation_config.mount_path}"
                )

            if conf.get("enabled", True):
                dc["services"][deployment_id] = entry

        # Merge infra service entries (Kafka, CouchDB, etc.) for enabled services
        infra_service_names = self._merge_infra_services(
            dc, deployments, external_networks
        )

        dc["networks"] = {}
        for network in external_networks:
            if network == "bitswan_external_testing":
                # Created by docker-compose as a regular bridge with outbound
                # internet access but no connectivity to internal services
                dc["networks"][network] = {"driver": "bridge"}
            else:
                dc["networks"][network] = {"external": True}
        dc_yaml = yaml.dump(dc)
        return dc_yaml, infra_service_names

    def _save_docker_compose(self, dc_yaml: str) -> None:
        """Save the generated docker-compose.yaml to the gitops directory for debugging."""
        dc_path = os.path.join(self.gitops_dir, "docker-compose.yaml")
        with open(dc_path, "w") as f:
            f.write(dc_yaml)
        logger.info(f"Saved docker-compose.yaml to {dc_path}")

    def _merge_infra_services(
        self, dc: dict, deployments: dict, external_networks: set
    ) -> list[str]:
        """Merge enabled infra service compose dicts into the main docker-compose.

        Returns the list of compose service names that were merged.
        """
        from app.services.infra_service import get_service, stage_for_deployment

        merged_service_names: list[str] = []

        # Collect unique (service_type, stage) pairs from all deployments
        seen: set[tuple[str, str]] = set()
        for dep_conf in deployments.values():
            dep_conf = dep_conf or {}
            dep_services = dep_conf.get("services")
            if not dep_services:
                continue
            dep_stage = dep_conf.get("stage") or "production"
            mapped_stage = stage_for_deployment(dep_stage)
            for svc_type, svc_conf in dep_services.items():
                enabled = (
                    svc_conf.get("enabled", True)
                    if isinstance(svc_conf, dict)
                    else bool(svc_conf)
                )
                if enabled:
                    seen.add((svc_type, mapped_stage))

        # For each enabled service, generate and merge its compose dict
        for svc_type, svc_stage in seen:
            try:
                svc = get_service(svc_type, self.workspace_name, stage=svc_stage)
            except ValueError:
                logger.warning(
                    f"Unknown service type '{svc_type}', skipping compose merge"
                )
                continue

            if not svc.is_enabled():
                logger.warning(
                    f"{svc.display_name} is declared by a deployment but not enabled "
                    f"(secrets file missing at {svc.secrets_file_path}). "
                    f"Skipping compose merge — this service will NOT run."
                )
                continue

            # Hook for services that need extra config before compose generation.
            svc.ensure_config()

            svc_compose = svc._generate_compose_dict()

            # Merge services
            for svc_name, svc_entry in svc_compose.get("services", {}).items():
                if svc_name not in dc["services"]:
                    dc["services"][svc_name] = svc_entry
                    merged_service_names.append(svc_name)

            # Merge volumes
            svc_volumes = svc_compose.get("volumes", {})
            if svc_volumes:
                if "volumes" not in dc:
                    dc["volumes"] = {}
                for vol_name, vol_conf in svc_volumes.items():
                    if vol_name not in dc["volumes"]:
                        dc["volumes"][vol_name] = vol_conf

            # Collect networks
            for net_name, net_conf in svc_compose.get("networks", {}).items():
                if net_conf and net_conf.get("external"):
                    external_networks.add(net_name)

        return merged_service_names
