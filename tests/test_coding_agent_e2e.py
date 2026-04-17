"""
E2E tests for coding agent functionality.

Tests the agent API endpoints: deployments, exec, logs, build-and-restart,
worktree sync, and agent session management.
"""

from __future__ import annotations

import json
import time
from typing import Optional

import pytest

from e2e_helpers import ssh_run, gitops_exec, WORKSPACE, GITOPS_CONTAINER

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# Agent Deployment API
# ---------------------------------------------------------------------------


class TestAgentDeployments:
    """Test the /agent/deployments endpoints used by the coding agent CLI.

    The agent API uses a separate secret (BITSWAN_GITOPS_AGENT_SECRET)
    discovered from the running coding-agent container. If no agent container
    exists, these endpoints return 401 — which we test for.
    """

    @pytest.fixture(autouse=True)
    def setup(self, api, check_gitops_running):
        self.api = api
        # Discover the agent secret — try the gitops secret first
        # (it may work if BITSWAN_GITOPS_AGENT_SECRET is not set)
        self._agent_secret = self.api.secret
        # Check if there's a coding agent container with its own secret
        result = ssh_run(
            'docker inspect %s-coding-agent --format '
            '"{{range .Config.Env}}{{println .}}{{end}}" 2>/dev/null || true' % WORKSPACE
        )
        for line in result.stdout.splitlines():
            if line.startswith("BITSWAN_GITOPS_AGENT_SECRET="):
                self._agent_secret = line.split("=", 1)[1]
                break
        self._has_agent = WORKSPACE + "-coding-agent" in (
            ssh_run("docker ps --format '{{.Names}}'").stdout
        )

    def _agent_get(self, path: str, timeout: int = 30):
        """Make a GET request to the agent API."""
        result = gitops_exec(
            f"curl -s -w '\\n%{{http_code}}' "
            f"-H 'Authorization: Bearer {self._agent_secret}' "
            f"http://localhost:8079/agent{path}",
            timeout=timeout,
        )
        lines = result.stdout.strip().rsplit("\n", 1)
        if len(lines) == 2:
            return lines[1], lines[0]
        return "0", result.stdout

    def _agent_post(self, path: str, data: Optional[str] = None, timeout: int = 30):
        """Make a POST request to the agent API."""
        data_flag = f"-d '{data}'" if data else ""
        result = gitops_exec(
            f"curl -s -w '\\n%{{http_code}}' "
            f"-H 'Authorization: Bearer {self._agent_secret}' "
            f"-H 'Content-Type: application/json' "
            f"-X POST {data_flag} "
            f"http://localhost:8079/agent{path}",
            timeout=timeout,
        )
        lines = result.stdout.strip().rsplit("\n", 1)
        if len(lines) == 2:
            return lines[1], lines[0]
        return "0", result.stdout

    def test_list_agent_deployments(self):
        """GET /agent/deployments returns deployment list or 401 (no agent)."""
        # Agent deployments endpoint requires worktree param
        code, body = self._agent_get("/deployments?worktree=main")
        if code == "401":
            pytest.skip("Agent secret not available (no coding-agent container)")
        assert code == "200", f"List deployments failed ({code}): {body}"
        data = json.loads(body)
        assert isinstance(data, list)

    def test_agent_deployment_has_fields(self):
        """Agent deployment list includes required fields."""
        code, body = self._agent_get("/deployments?worktree=main")
        if code == "401":
            pytest.skip("Agent secret not available")
        assert code == "200"
        data = json.loads(body)
        if not data:
            pytest.skip("No deployments")
        required = {"deployment_id", "stage", "automation_name"}
        for item in data:
            missing = required - set(item.keys())
            assert not missing, f"Missing fields {missing} in agent deployment"

    def test_agent_inspect_deployment(self):
        """GET /agent/deployments/{id}/inspect returns container info."""
        code, body = self._agent_get("/deployments?worktree=main")
        if code == "401":
            pytest.skip("Agent secret not available")
        data = json.loads(body)
        if not data:
            pytest.skip("No deployments")

        dep_id = data[0]["deployment_id"]
        code, body = self._agent_get(f"/deployments/{dep_id}/inspect")
        assert code in ("200", "404"), f"Inspect failed ({code}): {body}"

    def test_agent_inspect_env(self):
        """GET /agent/deployments/{id}/env returns environment variables."""
        code, body = self._agent_get("/deployments?worktree=main")
        if code == "401":
            pytest.skip("Agent secret not available")
        data = json.loads(body)
        if not data:
            pytest.skip("No deployments")

        dep_id = data[0]["deployment_id"]
        code, body = self._agent_get(f"/deployments/{dep_id}/env")
        assert code in ("200", "404"), f"Env inspect failed ({code}): {body}"

    def test_agent_logs(self):
        """GET /agent/deployments/{id}/logs returns SSE log stream."""
        code, body = self._agent_get("/deployments?worktree=main")
        if code == "401":
            pytest.skip("Agent secret not available")
        data = json.loads(body)
        if not data:
            pytest.skip("No deployments")

        dep_id = data[0]["deployment_id"]
        # Use timeout to not hang on SSE
        result = gitops_exec(
            f"timeout 3 curl -s "
            f"-H 'Authorization: Bearer {self.api.secret}' "
            f"'http://localhost:8079/agent/deployments/{dep_id}/logs?lines=5' "
            "|| true",
            timeout=10,
        )
        output = result.stdout
        # Should get SSE events or at least metadata
        assert "event:" in output or "data:" in output or len(output) == 0, \
            f"Unexpected log format: {output[:200]}"


# ---------------------------------------------------------------------------
# Agent Exec
# ---------------------------------------------------------------------------


class TestAgentExec:
    """Test executing commands in containers via the agent API."""

    @pytest.fixture(autouse=True)
    def setup(self, api, check_gitops_running):
        self.api = api

    def test_exec_simple_command(self):
        """POST /agent/deployments/{id}/exec runs a command."""
        result = gitops_exec(
            f"curl -s -w '\\n%{{http_code}}' "
            f"-H 'Authorization: Bearer {self.api.secret}' "
            f"http://localhost:8079/agent/deployments"
        )
        lines = result.stdout.strip().rsplit("\n", 1)
        if len(lines) != 2 or lines[1] != "200":
            pytest.skip("Cannot list deployments")

        data = json.loads(lines[0])
        if not data:
            pytest.skip("No deployments")

        dep_id = data[0]["deployment_id"]
        payload = json.dumps({"cmd": ["echo", "hello-e2e-test"]})
        result = gitops_exec(
            f"curl -s -w '\\n%{{http_code}}' "
            f"-H 'Authorization: Bearer {self.api.secret}' "
            f"-H 'Content-Type: application/json' "
            f"-X POST -d '{payload}' "
            f"http://localhost:8079/agent/deployments/{dep_id}/exec",
            timeout=15,
        )
        lines = result.stdout.strip().rsplit("\n", 1)
        if len(lines) == 2:
            body, code = lines
            assert code in ("200", "404"), f"Exec failed ({code}): {body}"
            if code == "200":
                assert "hello-e2e-test" in body, f"Command output missing: {body}"


# ---------------------------------------------------------------------------
# Coding Agent Session
# ---------------------------------------------------------------------------


class TestCodingAgentSession:
    """Test coding agent container management."""

    @pytest.fixture(autouse=True)
    def setup(self, api, check_gitops_running):
        self.api = api

    def test_ensure_coding_agent(self):
        """POST /worktrees/coding-agent/ensure creates/starts agent container."""
        result = gitops_exec(
            f"curl -s -w '\\n%{{http_code}}' "
            f"-H 'Authorization: Bearer {self.api.secret}' "
            f"-H 'Content-Type: application/json' "
            f"-X POST "
            f"http://localhost:8079/worktrees/coding-agent/ensure",
            timeout=30,
        )
        lines = result.stdout.strip().rsplit("\n", 1)
        if len(lines) == 2:
            body, code = lines
            # 200 success, 404 not configured, 500 no coding agent image
            assert code in ("200", "404", "500", "422"), \
                f"Ensure coding agent failed ({code}): {body}"


# ---------------------------------------------------------------------------
# Agent Worktree Sync
# ---------------------------------------------------------------------------


class TestAgentWorktreeSync:
    """Test the agent's worktree sync operations.

    Agent worktree endpoints require the agent secret, not the gitops secret.
    """

    WORKTREE_NAME = "e2e-agent-sync"

    @pytest.fixture(autouse=True)
    def setup(self, api, check_gitops_running):
        self.api = api
        # Create worktree via gitops API (uses gitops secret)
        payload = json.dumps({"branch_name": self.WORKTREE_NAME})
        self.api.post("/worktrees/create", data=payload, timeout=30)

    def test_worktree_log(self):
        """GET /agent/worktrees/{name}/log returns git log or 401 (no agent)."""
        result = gitops_exec(
            f"curl -s -w '\\n%{{http_code}}' "
            f"-H 'Authorization: Bearer {self.api.secret}' "
            f"http://localhost:8079/agent/worktrees/{self.WORKTREE_NAME}/log"
        )
        lines = result.stdout.strip().rsplit("\n", 1)
        if len(lines) == 2:
            body, code = lines
            if code == "401":
                pytest.skip("Agent secret not available (no coding-agent container)")
            assert code in ("200", "404"), f"Log failed ({code}): {body}"

    def test_worktree_diff(self):
        """GET /agent/worktrees/{name}/diff returns changes or 401."""
        result = gitops_exec(
            f"curl -s -w '\\n%{{http_code}}' "
            f"-H 'Authorization: Bearer {self.api.secret}' "
            f"http://localhost:8079/agent/worktrees/{self.WORKTREE_NAME}/diff"
        )
        lines = result.stdout.strip().rsplit("\n", 1)
        if len(lines) == 2:
            body, code = lines
            if code == "401":
                pytest.skip("Agent secret not available (no coding-agent container)")
            assert code in ("200", "404"), f"Diff failed ({code}): {body}"

    def teardown_method(self, method):
        if hasattr(self, "api"):
            self.api.delete(f"/worktrees/{self.WORKTREE_NAME}", timeout=30)
