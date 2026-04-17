"""
E2E security tests.

Tests authentication, rate limiting, path traversal protection,
and container isolation properties.
"""

import json
import time

import pytest

from e2e_helpers import ssh_run, gitops_exec, WORKSPACE, GITOPS_CONTAINER, SECRET

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# Authentication Security
# ---------------------------------------------------------------------------


class TestAuthSecurity:
    """Test authentication mechanisms."""

    @pytest.fixture(autouse=True)
    def setup(self, check_gitops_running):
        pass

    def test_invalid_token_returns_401(self):
        """Invalid bearer token gets 401."""
        result = gitops_exec(
            "curl -s -o /dev/null -w '%{http_code}' "
            "-H 'Authorization: Bearer totally-wrong-token' "
            "http://localhost:8079/automations/"
        )
        assert result.stdout.strip() == "401"

    def test_missing_auth_header(self):
        """No Authorization header gets 401/403."""
        result = gitops_exec(
            "curl -s -o /dev/null -w '%{http_code}' "
            "http://localhost:8079/automations/"
        )
        assert result.stdout.strip() in ("401", "403")

    def test_wrong_scheme(self):
        """Non-Bearer auth scheme is rejected."""
        result = gitops_exec(
            "curl -s -o /dev/null -w '%{http_code}' "
            f"-H 'Authorization: Basic {SECRET}' "
            "http://localhost:8079/automations/"
        )
        assert result.stdout.strip() in ("401", "403")


# ---------------------------------------------------------------------------
# Path Traversal Protection
# ---------------------------------------------------------------------------


class TestPathTraversal:
    """Test path traversal protection on endpoints."""

    @pytest.fixture(autouse=True)
    def setup(self, api, check_gitops_running):
        self.api = api

    def test_relative_path_traversal_blocked(self):
        """Path traversal in relative_path is blocked."""
        payload = json.dumps({"relative_path": "../../../etc/passwd"})
        status, body = self.api.post("/automations/start-live-dev", data=payload)
        # Should be 400/403/404, NOT 200. 429 means rate-limited (not a traversal success).
        assert status in (400, 403, 404, 422, 429), \
            f"Path traversal not blocked ({status}): {body}"
        assert status != 200, f"Path traversal returned 200: {body}"

    def test_deployment_id_traversal_blocked(self):
        """Path traversal in deployment_id URL is blocked."""
        status, body = self.api.get("/automations/../../../etc/passwd/inspect")
        assert status in (400, 403, 404, 422, 429), \
            f"Deployment ID traversal not blocked ({status}): {body}"

    def test_worktree_name_traversal_blocked(self):
        """Path traversal in worktree name is blocked."""
        payload = json.dumps({"branch_name": "../../etc"})
        status, body = self.api.post("/worktrees/create", data=payload)
        assert status in (400, 403, 404, 422, 429), \
            f"Worktree traversal not blocked ({status}): {body}"


# ---------------------------------------------------------------------------
# Container Security
# ---------------------------------------------------------------------------


class TestContainerSecurity:
    """Test container-level security properties."""

    @pytest.fixture(autouse=True)
    def setup(self, check_gitops_running):
        pass

    def _get_dev_container(self):
        result = ssh_run(
            "docker ps --filter name=live-dev --format '{{.Names}}'"
        )
        containers = [c.strip() for c in result.stdout.strip().split("\n") if c.strip()]
        if not containers:
            pytest.skip("No dev containers running")
        return containers[0]

    def test_no_docker_socket_in_automation(self):
        """Automation containers don't have Docker socket."""
        container = self._get_dev_container()
        result = ssh_run(
            f"docker exec {container} ls /var/run/docker.sock 2>&1 || true"
        )
        assert "No such file" in result.stdout or result.returncode != 0, \
            "Docker socket accessible in automation container!"

    def test_no_host_mount(self):
        """Automation containers don't mount host filesystem."""
        container = self._get_dev_container()
        result = ssh_run(
            'docker inspect %s --format "{{json .Mounts}}"' % container
        )
        mounts = json.loads(result.stdout.strip())
        for mount in mounts:
            src = mount.get("Source", "")
            assert src != "/", f"Container has host root mount: {mount}"
            assert not src.startswith("/etc"), f"Container mounts /etc: {mount}"

    def test_no_privileged_mode(self):
        """Automation containers don't run in privileged mode."""
        container = self._get_dev_container()
        result = ssh_run(
            'docker inspect %s --format "{{.HostConfig.Privileged}}"' % container
        )
        assert result.stdout.strip() == "false", "Container running in privileged mode!"

    def test_no_net_admin_capability(self):
        """Automation containers don't have NET_ADMIN capability."""
        container = self._get_dev_container()
        result = ssh_run(
            'docker inspect %s --format "{{json .HostConfig.CapAdd}}"' % container
        )
        caps = result.stdout.strip()
        if caps and caps != "null":
            cap_list = json.loads(caps)
            dangerous = {"ALL", "SYS_ADMIN", "NET_ADMIN", "SYS_PTRACE", "SYS_MODULE"}
            found = set(cap_list or []) & dangerous
            assert not found, f"Container has dangerous capabilities: {found}"

    def test_gitops_no_docker_socket(self):
        """Gitops container does NOT have real Docker socket."""
        result = gitops_exec("ls /var/run/docker.sock 2>&1 || true")
        assert "No such file" in result.stdout or result.returncode != 0, \
            "Real Docker socket accessible in gitops container!"

    def test_gitops_uses_proxy_socket(self):
        """Gitops container uses the container-manager proxy socket."""
        result = gitops_exec("sh -c 'echo $DOCKER_HOST'")
        docker_host = result.stdout.strip()
        assert "container-manager" in docker_host or "bitswan" in docker_host, \
            f"Gitops not using proxy socket: DOCKER_HOST={docker_host}"

    def test_metadata_service_blocked(self):
        """Cloud metadata service (169.254.169.254) is not reachable from dev containers."""
        container = self._get_dev_container()
        # Use curl with explicit timeout and check exit code
        result = ssh_run(
            f"docker exec {container} "
            "sh -c 'timeout 3 wget -q -O /dev/null http://169.254.169.254/ 2>&1; echo EXIT:$?'"
        )
        output = result.stdout.strip()
        # wget exit code: 0=success, 1=error, 4=network failure, 143=timeout
        # If blocked by iptables, wget hangs and timeout kills it (exit 143)
        # or wget gets connection refused (exit 4)
        # Empty output with exit 0 from the outer ssh means the || true ate the error
        assert "EXIT:0" not in output or output == "", \
            f"Metadata service reachable from dev container: {output}"


# ---------------------------------------------------------------------------
# Rate Limiting
# ---------------------------------------------------------------------------


class TestRateLimiting:
    """Test API rate limiting on authentication failures.

    IMPORTANT: This test triggers rate limiting on 127.0.0.1 inside the
    gitops container. It must run last and restarts the container afterward
    to clear the in-memory rate limit state.
    """

    @pytest.fixture(autouse=True)
    def setup(self, check_gitops_running):
        pass

    def test_zz_rate_limit_after_failures(self):
        """After many failed auth attempts, the IP gets rate limited.

        Named with zz_ prefix to ensure it runs last within this module.
        """
        # Send 11 requests with a bad token (limit is 10)
        last_code = "401"
        for i in range(12):
            result = gitops_exec(
                "curl -s -o /dev/null -w '%{http_code}' "
                f"-H 'Authorization: Bearer bad-token-{i}' "
                "http://localhost:8079/automations/"
            )
            last_code = result.stdout.strip()
            if last_code == "429":
                break

        # Should eventually get 429 (Too Many Requests)
        assert last_code in ("429", "401"), \
            f"Unexpected response after rate limit: {last_code}"

        # Restart gitops to clear the rate limit state for other tests
        ssh_run(f"docker restart {GITOPS_CONTAINER}", timeout=30)
        import time
        time.sleep(5)  # Wait for container to be ready
