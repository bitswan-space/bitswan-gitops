"""
E2E tests for the full deploy → access → manage flow.

Tests a complete lifecycle: deploy an automation, verify it's accessible
via HTTPS through VPN Traefik, stop/restart it, and check logs.
"""

from __future__ import annotations

import json
import time

import pytest

from e2e_helpers import ssh_run, WORKSPACE, SECRET

pytestmark = pytest.mark.e2e


def _get_gitops_container():
    """Find the gitops container name (may vary by compose project)."""
    result = ssh_run("docker ps --filter name=gitops --format '{{.Names}}'")
    containers = [c.strip() for c in result.stdout.strip().split("\n") if c.strip()]
    if not containers:
        pytest.skip("Gitops container not running")
    return containers[0]


def _api_call(gitops, method, path, data=None):
    """Make an API call to gitops."""
    data_flag = "-d '%s'" % data if data else ""
    result = ssh_run(
        "docker exec %s curl -s -w '\\n%%{http_code}' "
        "-H 'Authorization: Bearer %s' "
        "-H 'Content-Type: application/json' "
        "-X %s %s "
        "http://localhost:8079%s" % (gitops, SECRET, method, data_flag, path)
    )
    lines = result.stdout.strip().rsplit("\n", 1)
    if len(lines) == 2:
        try:
            return int(lines[1]), lines[0]
        except ValueError:
            return 0, result.stdout
    return 0, result.stdout


class TestDeployAndAccessFlow:
    """Test deploying and accessing an automation end-to-end."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.gitops = _get_gitops_container()

    def test_list_automations(self):
        """API returns running automations."""
        code, body = _api_call(self.gitops, "GET", "/automations/")
        assert code == 200, f"List failed ({code}): {body}"
        data = json.loads(body)
        assert isinstance(data, list)
        assert len(data) > 0, "No automations deployed"

    def test_deploy_returns_task_id(self):
        """Deploy returns a task_id for async tracking."""
        # Find an automation to deploy
        result = ssh_run(
            "docker exec %s find /workspace-repo/ "
            "-name automation.toml -not -path '*/worktrees/*' | head -1" % self.gitops
        )
        toml = result.stdout.strip()
        if not toml:
            pytest.skip("No automations to deploy")

        rel_path = toml.replace("/workspace-repo/", "").replace("/automation.toml", "")
        payload = json.dumps({"relative_path": rel_path})
        code, body = _api_call(
            self.gitops, "POST", "/automations/start-live-dev", payload
        )
        # 202 accepted (async), 409 already running
        assert code in (202, 409), f"Deploy failed ({code}): {body}"
        if code == 202:
            resp = json.loads(body)
            assert "task_id" in resp, f"Missing task_id in response: {resp}"
            assert "deployment_id" in resp, f"Missing deployment_id: {resp}"

    def test_container_on_correct_network(self):
        """Deployed container is on the workspace dev network."""
        result = ssh_run("docker ps --filter name=live-dev --format '{{.Names}}'")
        containers = [c.strip() for c in result.stdout.strip().split("\n") if c.strip()]
        if not containers:
            pytest.skip("No live-dev containers")

        result = ssh_run(
            'docker inspect %s --format "{{json .NetworkSettings.Networks}}"'
            % containers[0]
        )
        networks = json.loads(result.stdout.strip())
        dev_network = "%s-dev" % WORKSPACE
        assert dev_network in networks, (
            "Container not on dev network. Networks: %s" % list(networks.keys())
        )

    def test_stop_and_start(self):
        """Stop and start a deployed automation."""
        code, body = _api_call(self.gitops, "GET", "/automations/")
        data = json.loads(body)
        running = [d for d in data if d.get("state") == "running"]
        if not running:
            pytest.skip("No running automations")

        dep_id = running[0]["deployment_id"]

        # Stop
        code, body = _api_call(self.gitops, "POST", "/automations/%s/stop" % dep_id)
        assert code == 200, f"Stop failed ({code}): {body}"

        time.sleep(2)

        # Start
        code, body = _api_call(self.gitops, "POST", "/automations/%s/start" % dep_id)
        assert code == 200, f"Start failed ({code}): {body}"

        time.sleep(3)

    def test_restart(self):
        """Restart a deployed automation."""
        code, body = _api_call(self.gitops, "GET", "/automations/")
        data = json.loads(body)
        running = [d for d in data if d.get("state") == "running"]
        if not running:
            pytest.skip("No running automations")

        dep_id = running[0]["deployment_id"]
        code, body = _api_call(self.gitops, "POST", "/automations/%s/restart" % dep_id)
        assert code == 200, f"Restart failed ({code}): {body}"

    def test_log_stream_has_stream_field(self):
        """Log stream events include stdout/stderr stream field."""
        code, body = _api_call(self.gitops, "GET", "/automations/")
        data = json.loads(body)
        running = [d for d in data if d.get("state") == "running"]
        if not running:
            pytest.skip("No running automations")

        dep_id = running[0]["deployment_id"]

        # Get a few lines of logs
        result = ssh_run(
            "docker exec %s timeout 5 curl -s "
            "-H 'Authorization: Bearer %s' "
            "'http://localhost:8079/automations/%s/logs/stream?lines=5' "
            "|| true" % (self.gitops, SECRET, dep_id),
            timeout=15,
        )
        output = result.stdout
        if not output:
            pytest.skip("No log output")

        # Parse SSE and check for stream field
        for line in output.split("\n"):
            if line.startswith("data: ") and '"line"' in line:
                event = json.loads(line[6:])
                assert "stream" in event, f"Missing stream field: {event}"
                assert event["stream"] in (
                    "stdout",
                    "stderr",
                ), f"Invalid stream: {event['stream']}"
                break

    def test_deployment_history(self):
        """Deployment history is available for deployed automations."""
        code, body = _api_call(self.gitops, "GET", "/automations/")
        data = json.loads(body)
        if not data:
            pytest.skip("No automations")

        dep_id = data[0]["deployment_id"]
        code, body = _api_call(self.gitops, "GET", "/automations/%s/history" % dep_id)
        assert code in (200, 404), f"History failed ({code}): {body}"
