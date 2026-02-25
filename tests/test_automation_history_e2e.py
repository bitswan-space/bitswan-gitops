"""
End-to-end test for the /automations/{id}/history endpoint.

Starts the real uvicorn server (with lifespan and background cache warm-up),
creates a temp git repo with known commits, makes real HTTP requests, and
verifies correctness, pagination, and cache consistency under concurrency.
"""

import concurrent.futures
import os
import signal
import socket
import subprocess
import sys
import time

import httpx
import pytest
import yaml


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _git(repo_dir, *args):
    """Run a git command synchronously and assert success."""
    result = subprocess.run(
        ["git", *args],
        cwd=repo_dir,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"git {' '.join(args)} failed: {result.stderr}"
    return result.stdout.strip()


def _free_port():
    """Find a free TCP port."""
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    """
    Build a temp git repo with known bitswan.yaml commits, start the real
    uvicorn server (lifespan fires the background cache warm-up), yield
    connection details, then tear down.
    """
    tmp_path = tmp_path_factory.mktemp("gitops_e2e")

    # ---- build the git repo ------------------------------------------------
    gitops_dir = tmp_path / "gitops"
    gitops_dir.mkdir()

    _git(gitops_dir, "init")
    _git(gitops_dir, "config", "user.name", "Test")
    _git(gitops_dir, "config", "user.email", "test@test.com")

    bitswan = gitops_dir / "bitswan.yaml"

    def commit(yaml_content, message):
        bitswan.write_text(yaml.dump(yaml_content))
        _git(gitops_dir, "add", "bitswan.yaml")
        _git(gitops_dir, "commit", "-m", message)

    # Commit 1 — initial
    commit(
        {
            "deployments": {
                "myapp-dev": {
                    "checksum": "aaa111",
                    "stage": "dev",
                    "relative_path": "automations/myapp",
                    "active": True,
                },
            }
        },
        "Initial deployment",
    )

    # Commit 2 — new checksum
    commit(
        {
            "deployments": {
                "myapp-dev": {
                    "checksum": "bbb222",
                    "stage": "dev",
                    "relative_path": "automations/myapp",
                    "active": True,
                },
            }
        },
        "Update myapp to v2",
    )

    # Commit 3 — same checksum, different field → should NOT create a new
    #             history entry (the dedup filter skips unchanged checksums)
    commit(
        {
            "deployments": {
                "myapp-dev": {
                    "checksum": "bbb222",
                    "stage": "dev",
                    "relative_path": "automations/myapp",
                    "active": False,
                },
            }
        },
        "Disable myapp",
    )

    # Commit 4 — new checksum + stage change
    commit(
        {
            "deployments": {
                "myapp-dev": {
                    "checksum": "ccc333",
                    "stage": "staging",
                    "relative_path": "automations/myapp",
                    "active": True,
                },
            }
        },
        "Promote myapp to staging",
    )

    # Commit 5 — new checksum + tag_checksum
    commit(
        {
            "deployments": {
                "myapp-dev": {
                    "checksum": "ddd444",
                    "stage": "staging",
                    "relative_path": "automations/myapp",
                    "active": True,
                    "tag_checksum": "tag999",
                },
            }
        },
        "Rebuild myapp image",
    )

    # ---- start the server --------------------------------------------------
    port = _free_port()
    secret = "test-secret-token"

    env = os.environ.copy()
    env["BITSWAN_GITOPS_DIR"] = str(tmp_path)
    env["BITSWAN_GITOPS_SECRET"] = secret
    # Ensure local git (no nsenter) and no MQTT side-effects
    env.pop("HOST_PATH", None)
    env.pop("MQTT_BROKER", None)

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "info",
        ],
        cwd=str(os.path.join(os.path.dirname(__file__), "..")),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    base_url = f"http://127.0.0.1:{port}"
    headers = {"Authorization": f"Bearer {secret}"}

    # Wait for the server to accept connections
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            r = httpx.get(f"{base_url}/openapi.json", timeout=1)
            if r.status_code == 200:
                break
        except httpx.ConnectError:
            pass
        time.sleep(0.3)
    else:
        output = proc.stdout.read().decode() if proc.stdout else ""
        proc.kill()
        pytest.fail(f"Server did not start within 30 s.\nOutput:\n{output}")

    yield base_url, headers

    # ---- tear down ---------------------------------------------------------
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------


class TestAutomationHistoryE2E:
    """
    Real HTTP tests against a running server.
    The lifespan background task (warm_history_cache) may still be running
    when the first request arrives — this is intentional: it exercises the
    concurrent-access path.
    """

    def test_history_correct_entries(self, server):
        """Should return one entry per unique consecutive checksum."""
        base_url, headers = server

        r = httpx.get(
            f"{base_url}/automations/myapp-dev/history",
            headers=headers,
            timeout=60,
        )
        assert r.status_code == 200
        data = r.json()

        # 5 commits but only 4 distinct consecutive checksums
        assert data["total"] == 4

        checksums = [e["checksum"] for e in data["items"]]
        assert checksums == ["ddd444", "ccc333", "bbb222", "aaa111"]

    def test_newest_entry_fields(self, server):
        """The most recent entry should carry all expected fields."""
        base_url, headers = server

        r = httpx.get(
            f"{base_url}/automations/myapp-dev/history?page_size=1",
            headers=headers,
            timeout=60,
        )
        entry = r.json()["items"][0]

        assert len(entry["commit"]) == 40  # full git SHA
        assert isinstance(entry["author"], str)
        assert isinstance(entry["date"], str)
        assert entry["message"] == "Rebuild myapp image"
        assert entry["checksum"] == "ddd444"
        assert entry["stage"] == "staging"
        assert entry["relative_path"] == "automations/myapp"
        assert entry["active"] is True
        assert entry["tag_checksum"] == "tag999"

    def test_pagination(self, server):
        """Pages should slice the full history correctly."""
        base_url, headers = server

        # Page 1 of 2
        r = httpx.get(
            f"{base_url}/automations/myapp-dev/history?page=1&page_size=2",
            headers=headers,
            timeout=60,
        )
        p1 = r.json()
        assert p1["total"] == 4
        assert p1["page"] == 1
        assert p1["page_size"] == 2
        assert p1["total_pages"] == 2
        assert len(p1["items"]) == 2
        assert p1["items"][0]["checksum"] == "ddd444"
        assert p1["items"][1]["checksum"] == "ccc333"

        # Page 2 of 2
        r = httpx.get(
            f"{base_url}/automations/myapp-dev/history?page=2&page_size=2",
            headers=headers,
            timeout=60,
        )
        p2 = r.json()
        assert len(p2["items"]) == 2
        assert p2["items"][0]["checksum"] == "bbb222"
        assert p2["items"][1]["checksum"] == "aaa111"

    def test_nonexistent_deployment(self, server):
        """A deployment that was never in bitswan.yaml should return empty."""
        base_url, headers = server

        r = httpx.get(
            f"{base_url}/automations/no-such-app/history",
            headers=headers,
            timeout=60,
        )
        assert r.status_code == 200
        data = r.json()
        assert data["total"] == 0
        assert data["items"] == []

    def test_cache_consistency(self, server):
        """Repeated requests must return identical data."""
        base_url, headers = server

        results = []
        for _ in range(3):
            r = httpx.get(
                f"{base_url}/automations/myapp-dev/history",
                headers=headers,
                timeout=60,
            )
            assert r.status_code == 200
            results.append(r.json())

        assert results[0] == results[1] == results[2]

    def test_concurrent_requests_no_race(self, server):
        """Fire 10 concurrent requests — all must succeed with the same data."""
        base_url, headers = server
        url = f"{base_url}/automations/myapp-dev/history"

        def fetch():
            return httpx.get(url, headers=headers, timeout=60)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(fetch) for _ in range(10)]
            responses = [f.result() for f in futures]

        bodies = []
        for resp in responses:
            assert resp.status_code == 200
            bodies.append(resp.json())

        # Every response should be identical
        for body in bodies[1:]:
            assert body == bodies[0]
