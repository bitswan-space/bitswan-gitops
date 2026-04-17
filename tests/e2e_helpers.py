"""
Shared fixtures for BitSwan E2E test suite.

Tests run against a live BitSwan instance. Configure via env vars:
  BITSWAN_TEST_HOST  — SSH host (e.g., root@88.99.15.208)
  BITSWAN_TEST_GITOPS_URL — internal gitops URL (default: http://localhost:8079)
  BITSWAN_TEST_SECRET — gitops Bearer token
  BITSWAN_TEST_WORKSPACE — workspace name (e.g., editor-network-test)
  BITSWAN_TEST_CONTAINER — gitops container name
"""

from __future__ import annotations

import os
import subprocess
from typing import Optional, Tuple

import pytest


# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------

TEST_HOST = os.environ.get("BITSWAN_TEST_HOST", "root@88.99.15.208")
GITOPS_URL = os.environ.get("BITSWAN_TEST_GITOPS_URL", "http://localhost:8079")
SECRET = os.environ.get(
    "BITSWAN_TEST_SECRET",
    "s9QCPa2yhXcC0hf4ZbIz56vB4At7e1CCquSCIVlGKSTW4wou8Huztj5KueR0O5Xq",
)
WORKSPACE = os.environ.get("BITSWAN_TEST_WORKSPACE", "editor-network-test")
GITOPS_CONTAINER = os.environ.get(
    "BITSWAN_TEST_CONTAINER", "editor-network-test-site-bitswan-gitops-1"
)


# ---------------------------------------------------------------------------
# SSH / Docker helpers
# ---------------------------------------------------------------------------


def ssh_run(cmd: str, timeout: int = 30) -> subprocess.CompletedProcess:
    """Run a command on the test host via SSH."""
    result = subprocess.run(
        [
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            f"ConnectTimeout={timeout}",
            TEST_HOST,
            cmd,
        ],
        capture_output=True,
        text=True,
        timeout=timeout + 10,
    )
    return result


def docker_exec(
    container: str, cmd: str, timeout: int = 30
) -> subprocess.CompletedProcess:
    """Run a command inside a Docker container on the test host."""
    return ssh_run(f"docker exec {container} {cmd}", timeout=timeout)


def gitops_exec(cmd: str, timeout: int = 30) -> subprocess.CompletedProcess:
    """Run a command inside the gitops container."""
    return docker_exec(GITOPS_CONTAINER, cmd, timeout=timeout)


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------


class GitOpsAPI:
    """HTTP client for the GitOps API, proxied through docker exec + curl."""

    def __init__(self, base_url: str, secret: str, container: str):
        self.base_url = base_url
        self.secret = secret
        self.container = container

    def _curl(
        self,
        method: str,
        path: str,
        data: Optional[str] = None,
        timeout: int = 30,
        raw: bool = False,
    ) -> Tuple[int, str]:
        """Execute a curl request inside the gitops container."""
        url = f"{self.base_url}{path}"
        cmd_parts = [
            "curl",
            "-s",
            "-w",
            "'\\n%{http_code}'",
            "-X",
            method,
            "-H",
            f"'Authorization: Bearer {self.secret}'",
            "-H",
            "'Content-Type: application/json'",
        ]
        if data:
            cmd_parts.extend(["-d", f"'{data}'"])
        cmd_parts.append(f"'{url}'")
        cmd = " ".join(cmd_parts)
        result = docker_exec(self.container, cmd, timeout=timeout)
        output = result.stdout.strip()
        # Last line is the HTTP status code
        lines = output.rsplit("\n", 1)
        if len(lines) == 2:
            body, status = lines
            try:
                return int(status), body
            except ValueError:
                return 0, output
        return 0, output

    def get(self, path: str, timeout: int = 30) -> Tuple[int, str]:
        return self._curl("GET", path, timeout=timeout)

    def post(
        self, path: str, data: Optional[str] = None, timeout: int = 30
    ) -> Tuple[int, str]:
        return self._curl("POST", path, data=data, timeout=timeout)

    def delete(self, path: str, timeout: int = 30) -> Tuple[int, str]:
        return self._curl("DELETE", path, timeout=timeout)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def api() -> GitOpsAPI:
    """GitOps API client."""
    return GitOpsAPI(GITOPS_URL, SECRET, GITOPS_CONTAINER)


@pytest.fixture(scope="session")
def workspace_name() -> str:
    return WORKSPACE


@pytest.fixture(scope="session")
def check_server_accessible():
    """Verify the test server is reachable before running tests."""
    result = ssh_run("echo ok", timeout=10)
    if result.returncode != 0:
        pytest.skip(f"Test server {TEST_HOST} not accessible: {result.stderr}")


@pytest.fixture(scope="session")
def check_gitops_running(check_server_accessible):
    """Verify gitops container is running."""
    result = ssh_run(
        f"docker inspect {GITOPS_CONTAINER} --format '{{{{.State.Running}}}}'"
    )
    if "true" not in result.stdout:
        pytest.skip(f"Gitops container {GITOPS_CONTAINER} not running")
