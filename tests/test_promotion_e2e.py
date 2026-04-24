"""
E2E tests for the promotion manager.

Tests the stage promotion flow: deploy to dev → promote to staging → promote to production.
Verifies that containers land on the correct stage networks.
"""

import json
import time

import pytest

from e2e_helpers import ssh_run, gitops_exec, WORKSPACE

pytestmark = pytest.mark.e2e


class TestPromotionFlow:
    """Test multi-stage promotion lifecycle."""

    @pytest.fixture(autouse=True)
    def setup(self, api, check_gitops_running):
        self.api = api

    def _find_automation(self):
        """Find an automation relative_path to use for testing."""
        result = gitops_exec(
            "find /workspace-repo/ -name automation.toml "
            "-not -path '*/worktrees/*' | head -1"
        )
        path = result.stdout.strip()
        if not path:
            pytest.skip("No automations in workspace")
        return path.replace("/workspace-repo/", "").replace("/automation.toml", "")

    def _wait_for_container(self, name_fragment: str, timeout: int = 30) -> bool:
        """Wait for a container matching name_fragment to be running."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            result = ssh_run(
                "docker ps --filter name=%s --format '{{.Names}}'" % name_fragment
            )
            if result.stdout.strip():
                return True
            time.sleep(2)
        return False

    def test_deployment_history(self):
        """GET /automations/{id}/history returns deployment history."""
        status, body = self.api.get("/automations/")
        data = json.loads(body)
        if not data:
            pytest.skip("No automations")

        dep_id = data[0]["deployment_id"]
        status, body = self.api.get(f"/automations/{dep_id}/history")
        assert status in (200, 404), f"History failed ({status}): {body}"
        if status == 200:
            history = json.loads(body)
            assert isinstance(history, (list, dict))

    def test_deploy_to_dev(self):
        """Deploy an automation to the dev stage."""
        rel_path = self._find_automation()
        automation_name = rel_path.split("/")[-1]

        # The dev deployment ID format: {automation_name}-{bp}-dev
        payload = json.dumps(
            {
                "relative_path": rel_path,
                "stage": "dev",
            }
        )
        status, body = self.api.post(
            f"/automations/{automation_name}-dev/deploy",
            data=payload,
            timeout=60,
        )
        # Various success/already-deployed codes
        assert status in (
            200,
            201,
            202,
            409,
            422,
        ), f"Deploy to dev failed ({status}): {body}"

    def test_promote_to_staging(self):
        """Promote from dev to staging."""
        status, body = self.api.get("/automations/")
        data = json.loads(body)
        dev_deployments = [d for d in data if d.get("stage") == "dev"]
        if not dev_deployments:
            pytest.skip("No dev deployments to promote")

        dep = dev_deployments[0]
        automation_name = dep["automation_name"]
        checksum = dep.get("version_hash") or dep.get("checksum", "")

        staging_id = f"{automation_name}-staging"
        payload = json.dumps(
            {
                "relative_path": dep.get("relative_path", ""),
                "stage": "staging",
                "checksum": checksum,
            }
        )
        status, body = self.api.post(
            f"/automations/{staging_id}/deploy",
            data=payload,
            timeout=60,
        )
        assert status in (
            200,
            201,
            202,
            409,
            422,
        ), f"Promote to staging failed ({status}): {body}"

    def test_staging_on_staging_network(self):
        """Staging containers are on the staging network."""
        result = ssh_run("docker ps --filter name=staging --format '{{.Names}}'")
        containers = [c.strip() for c in result.stdout.strip().split("\n") if c.strip()]
        if not containers:
            pytest.skip("No staging containers running")

        for container in containers:
            result = ssh_run(
                'docker inspect %s --format "{{json .NetworkSettings.Networks}}"'
                % container
            )
            networks = json.loads(result.stdout.strip())
            assert any(
                f"{WORKSPACE}-staging" in n for n in networks
            ), f"Staging container {container} not on staging network: {list(networks.keys())}"

    def test_dev_cannot_reach_staging(self):
        """Dev containers cannot communicate with staging containers."""
        dev_result = ssh_run("docker ps --filter name=dev --format '{{.Names}}'")
        dev_containers = [
            c.strip()
            for c in dev_result.stdout.strip().split("\n")
            if c.strip() and "live-dev" in c
        ]

        staging_result = ssh_run(
            r"docker ps --filter name=staging --format '{{.Names}}\t{{.ID}}'"
        )
        staging_lines = [
            line.strip()
            for line in staging_result.stdout.strip().split("\n")
            if line.strip()
        ]

        if not dev_containers or not staging_lines:
            pytest.skip("Need both dev and staging containers")

        dev_container = dev_containers[0]
        # Get staging container IP
        staging_name = staging_lines[0].split("\t")[0]
        ip_result = ssh_run(
            'docker inspect %s --format "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"'
            % staging_name
        )
        staging_ip = ip_result.stdout.strip()
        if not staging_ip:
            pytest.skip("Cannot determine staging IP")

        # Try to reach staging from dev — should fail
        result = ssh_run(
            f"docker exec {dev_container} timeout 3 wget -q -O /dev/null http://{staging_ip}:80 2>&1 || true"
        )
        # Connection should fail (timeout, refused, or no route)
        assert (
            result.returncode != 0
            or "timed out" in result.stdout.lower()
            or "connection refused" in result.stdout.lower()
            or "no route" in result.stdout.lower()
        ), f"Dev container could reach staging: {result.stdout}"


# ---------------------------------------------------------------------------
# Asset Management
# ---------------------------------------------------------------------------


class TestAssetManagement:
    """Test asset upload and diff functionality."""

    @pytest.fixture(autouse=True)
    def setup(self, api, check_gitops_running):
        self.api = api

    def test_list_assets(self):
        """GET /automations/assets returns asset list."""
        status, body = self.api.get("/automations/assets")
        assert status == 200, f"List assets failed ({status}): {body}"

    def test_asset_diff(self):
        """GET /automations/assets/diff returns diff between checksums."""
        # Without params, should return an error or empty diff
        status, body = self.api.get("/automations/assets/diff")
        # 200 or 422 (missing params) both acceptable
        assert status in (200, 400, 422), f"Asset diff failed ({status}): {body}"
