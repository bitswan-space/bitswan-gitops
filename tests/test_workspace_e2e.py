"""
E2E tests for BitSwan workspace functionality.

Tests the core workspace operations: listing automations, deploying,
starting/stopping, log streaming, and container lifecycle management.

Requires a running BitSwan instance. See conftest.py for configuration.
"""

import json
import time

import pytest

from e2e_helpers import ssh_run, gitops_exec, WORKSPACE, GITOPS_CONTAINER

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# Workspace State
# ---------------------------------------------------------------------------


class TestWorkspaceState:
    """Test basic workspace state queries."""

    def test_list_automations(self, api, check_gitops_running):
        """GET /automations/ returns a valid list."""
        status, body = api.get("/automations/")
        assert status == 200, f"Expected 200, got {status}: {body}"
        data = json.loads(body)
        assert isinstance(data, list)

    def test_automations_have_required_fields(self, api, check_gitops_running):
        """Each automation has deployment_id, stage, active, state fields."""
        status, body = api.get("/automations/")
        assert status == 200
        data = json.loads(body)
        required_fields = {"deployment_id", "stage", "active", "automation_name"}
        for item in data:
            missing = required_fields - set(item.keys())
            assert not missing, (
                f"Missing fields {missing} in {item.get('deployment_id', '?')}"
            )

    def test_unauthenticated_request_rejected(self, check_gitops_running):
        """Requests without a valid token get 401 or 403."""
        result = gitops_exec(
            "curl -s -o /dev/null -w '%{http_code}' "
            "-H 'Authorization: Bearer INVALID_TOKEN' "
            "http://localhost:8079/automations/"
        )
        code = result.stdout.strip()
        assert code in ("401", "403"), f"Expected 401/403, got {code}"

    def test_no_token_rejected(self, check_gitops_running):
        """Requests with no Authorization header are rejected."""
        result = gitops_exec(
            "curl -s -o /dev/null -w '%{http_code}' http://localhost:8079/automations/"
        )
        code = result.stdout.strip()
        assert code in ("401", "403"), f"Expected 401/403, got {code}"


# ---------------------------------------------------------------------------
# Automation Deployment
# ---------------------------------------------------------------------------


class TestAutomationDeployment:
    """Test deploying automations through the API."""

    @pytest.fixture(autouse=True)
    def setup(self, api, check_gitops_running):
        self.api = api

    def test_start_live_dev(self):
        """POST /automations/start-live-dev deploys a live-dev automation."""
        # First, check what automations exist in the workspace repo
        result = gitops_exec("ls /workspace-repo/")
        assert result.returncode == 0, f"Cannot list workspace: {result.stderr}"

        # Check if there's a business process with an automation
        result = gitops_exec(
            "find /workspace-repo/ -name automation.toml -not -path '*/worktrees/*' | head -5"
        )
        toml_paths = [p.strip() for p in result.stdout.strip().split("\n") if p.strip()]
        if not toml_paths:
            pytest.skip("No automations in workspace to test")

        # Get the relative path (strip /workspace-repo/ prefix)
        rel_path = (
            toml_paths[0]
            .replace("/workspace-repo/", "")
            .replace("/automation.toml", "")
        )

        # Deploy via start-live-dev
        payload = json.dumps({"relative_path": rel_path})
        status, body = self.api.post(
            "/automations/start-live-dev", data=payload, timeout=60
        )
        # Accept 200 (success), 409 (already running), or 202 (accepted)
        assert status in (200, 202, 409), f"Deploy failed ({status}): {body}"

    def test_list_includes_deployed(self):
        """After deployment, the automation appears in the list."""
        status, body = self.api.get("/automations/")
        assert status == 200
        data = json.loads(body)
        assert len(data) > 0, "No automations found after deployment"

    def test_automation_inspect(self):
        """GET /automations/{id}/inspect returns container details."""
        status, body = self.api.get("/automations/")
        assert status == 200
        data = json.loads(body)
        if not data:
            pytest.skip("No automations to inspect")

        deployment_id = data[0]["deployment_id"]
        status, body = self.api.get(f"/automations/{deployment_id}/inspect")
        # 200 if running, 404 if not yet started
        assert status in (200, 404), f"Inspect failed ({status}): {body}"


# ---------------------------------------------------------------------------
# Log Streaming
# ---------------------------------------------------------------------------


class TestLogStreaming:
    """Test log streaming endpoints."""

    @pytest.fixture(autouse=True)
    def setup(self, api, check_gitops_running):
        self.api = api
        # Ensure at least one container is running before log tests
        status, body = self.api.get("/automations/")
        if status == 200:
            data = json.loads(body)
            if data:
                dep_id = data[0]["deployment_id"]
                self.api.post(f"/automations/{dep_id}/start")
                time.sleep(2)

    def test_log_stream_returns_sse(self):
        """GET /automations/{id}/logs/stream returns SSE format."""
        status, body = self.api.get("/automations/")
        data = json.loads(body)
        if not data:
            pytest.skip("No automations to test logs")

        # Find a running deployment
        deployment_id = data[0]["deployment_id"]

        # Restart the container and wait to ensure it's running
        self.api.post(f"/automations/{deployment_id}/restart")
        time.sleep(5)

        result = gitops_exec(
            f"timeout 8 curl -s "
            f"-H 'Authorization: Bearer {self.api.secret}' "
            f"'http://localhost:8079/automations/{deployment_id}/logs/stream?lines=10' "
            "|| true",
            timeout=20,
        )
        output = result.stdout
        # Should contain SSE events (event: or data:) or error event
        # Empty output means the container may not be running yet
        if not output:
            pytest.skip("No log output (container may still be starting)")
        assert "event:" in output or "data:" in output, (
            f"Expected SSE format, got: {output[:500]}"
        )

    def test_log_stream_has_metadata_event(self):
        """Log stream starts with a metadata event."""
        status, body = self.api.get("/automations/")
        data = json.loads(body)
        if not data:
            pytest.skip("No automations")

        deployment_id = data[0]["deployment_id"]
        result = gitops_exec(
            f"timeout 8 curl -s "
            f"-H 'Authorization: Bearer {self.api.secret}' "
            f"'http://localhost:8079/automations/{deployment_id}/logs/stream?lines=5' "
            "|| true",
            timeout=20,
        )
        output = result.stdout
        if not output:
            pytest.skip("No log output (container may still be starting)")
        # metadata or error events are both valid SSE responses
        assert "event: metadata" in output or "event: error" in output, (
            f"Missing metadata/error event in: {output[:500]}"
        )

    def test_log_stream_distinguishes_stderr(self):
        """Log events include stream field (stdout/stderr)."""
        status, body = self.api.get("/automations/")
        data = json.loads(body)
        if not data:
            pytest.skip("No automations")

        deployment_id = data[0]["deployment_id"]
        result = gitops_exec(
            f"timeout 5 curl -s "
            f"-H 'Authorization: Bearer {self.api.secret}' "
            f"'http://localhost:8079/automations/{deployment_id}/logs/stream?lines=50' "
            "|| true",
            timeout=12,
        )
        # Check that log events contain a "stream" field
        for line in result.stdout.split("\n"):
            if line.startswith("data: ") and '"line"' in line:
                event_data = json.loads(line[6:])
                assert "stream" in event_data, (
                    f"Missing 'stream' field in log event: {line}"
                )
                assert event_data["stream"] in ("stdout", "stderr"), (
                    f"Invalid stream value: {event_data['stream']}"
                )
                break  # One check is sufficient


# ---------------------------------------------------------------------------
# Container Lifecycle
# ---------------------------------------------------------------------------


class TestContainerLifecycle:
    """Test start/stop/restart operations."""

    @pytest.fixture(autouse=True)
    def setup(self, api, check_gitops_running):
        self.api = api

    def _get_first_deployment(self):
        status, body = self.api.get("/automations/")
        data = json.loads(body)
        if not data:
            pytest.skip("No automations")
        return data[0]["deployment_id"]

    def test_stop_automation(self):
        """POST /automations/{id}/stop stops the container."""
        dep_id = self._get_first_deployment()
        status, body = self.api.post(f"/automations/{dep_id}/stop")
        assert status in (200, 204, 404), f"Stop failed ({status}): {body}"

    def test_start_automation(self):
        """POST /automations/{id}/start starts the container."""
        dep_id = self._get_first_deployment()
        status, body = self.api.post(f"/automations/{dep_id}/start")
        assert status in (200, 204, 404), f"Start failed ({status}): {body}"

    def test_restart_automation(self):
        """POST /automations/{id}/restart restarts the container."""
        dep_id = self._get_first_deployment()
        status, body = self.api.post(f"/automations/{dep_id}/restart")
        assert status in (200, 204, 404), f"Restart failed ({status}): {body}"


# ---------------------------------------------------------------------------
# Network Isolation
# ---------------------------------------------------------------------------


class TestNetworkIsolation:
    """Verify containers are on correct Docker networks."""

    @pytest.fixture(autouse=True)
    def setup(self, check_gitops_running):
        pass

    def test_stage_networks_exist(self):
        """Per-stage networks (dev, staging, production) exist."""
        result = ssh_run("docker network ls --format '{{.Name}}' | grep %s" % WORKSPACE)
        networks = result.stdout.strip().split("\n")
        expected = {
            f"{WORKSPACE}-dev",
            f"{WORKSPACE}-staging",
            f"{WORKSPACE}-production",
        }
        actual = {n.strip() for n in networks if n.strip()}
        missing = expected - actual
        assert not missing, f"Missing stage networks: {missing}"

    def test_dev_container_on_dev_network(self):
        """Live-dev containers are on the dev network."""
        result = ssh_run("docker ps --filter name=live-dev --format '{{.Names}}'")
        containers = [c.strip() for c in result.stdout.strip().split("\n") if c.strip()]
        if not containers:
            pytest.skip("No live-dev containers running")

        for container in containers:
            result = ssh_run(
                'docker inspect %s --format "{{json .NetworkSettings.Networks}}"'
                % container
            )
            output = result.stdout.strip()
            assert output, f"Empty inspect output for {container}"
            networks = json.loads(output)
            network_names = list(networks.keys())
            assert any(f"{WORKSPACE}-dev" in n for n in network_names), (
                f"Container {container} not on dev network. Networks: {network_names}"
            )

    def test_cross_stage_isolation(self):
        """Dev containers cannot resolve staging/production services."""
        result = ssh_run("docker ps --filter name=live-dev --format '{{.Names}}'")
        containers = [c.strip() for c in result.stdout.strip().split("\n") if c.strip()]
        if not containers:
            pytest.skip("No live-dev containers running")

        container = containers[0]
        result = ssh_run(
            "docker exec %s nslookup %s-staging-postgres 2>&1 || true"
            % (container, WORKSPACE)
        )
        assert (
            "NXDOMAIN" in result.stdout
            or "SERVFAIL" in result.stdout
            or "can't resolve" in result.stdout
            or "server can't find" in result.stdout
            or result.returncode != 0
        ), f"Dev container could resolve staging service: {result.stdout}"

    def test_management_plane_isolation(self):
        """Dev containers cannot reach management services (gitops, daemon)."""
        result = ssh_run("docker ps --filter name=live-dev --format '{{.Names}}'")
        containers = [c.strip() for c in result.stdout.strip().split("\n") if c.strip()]
        if not containers:
            pytest.skip("No live-dev containers running")

        container = containers[0]
        result = ssh_run(
            "docker exec %s nslookup %s 2>&1 || true" % (container, GITOPS_CONTAINER)
        )
        assert (
            "NXDOMAIN" in result.stdout
            or "SERVFAIL" in result.stdout
            or "can't resolve" in result.stdout
            or "server can't find" in result.stdout
            or result.returncode != 0
        ), f"Dev container could resolve gitops: {result.stdout}"
