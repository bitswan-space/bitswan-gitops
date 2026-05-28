"""
End-to-end tests for the /snapshots API.

Starts a real uvicorn server (same as test_automation_history_e2e.py).
Infra services (postgres/couchdb/minio) are not running in CI so the
backup path takes the "service not enabled" branch, producing empty
placeholder files. This is enough to exercise the full task/SSE/manifest
lifecycle. Tests that require real Docker containers are marked skip.
"""

import json
import os
import signal
import socket
import subprocess
import sys
import time

import httpx
import pytest


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _free_port():
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _poll_task(base_url, headers, task_id, timeout=30) -> dict:
    """Poll GET /snapshots/tasks until the task is no longer running/pending."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = httpx.get(f"{base_url}/snapshots/tasks", headers=headers, timeout=5)
        assert r.status_code == 200
        tasks = r.json()
        for t in tasks:
            if t["task_id"] == task_id:
                if t["status"] not in ("pending", "running"):
                    return t
        time.sleep(0.3)
    pytest.fail(f"Task {task_id} did not finish within {timeout}s")


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    """
    Start the real uvicorn server with a temp BITSWAN_GITOPS_DIR.
    No git repo needed for snapshot tests.
    """
    tmp_path = tmp_path_factory.mktemp("snapshot_e2e")
    gitops_dir = tmp_path / "gitops"
    gitops_dir.mkdir()

    # Create minimal bitswan.yaml so automation service doesn't crash
    (gitops_dir / "bitswan.yaml").write_text("deployments: {}\n")

    # Set up a fake workspace repo with two worktrees so worktree-snapshot
    # validation tests can run without Docker.
    workspace_repo_dir = tmp_path / "workspace-repo"
    (workspace_repo_dir / "worktrees" / "wt-a").mkdir(parents=True)
    (workspace_repo_dir / "worktrees" / "wt-b").mkdir(parents=True)

    port = _free_port()
    secret = "test-snapshot-secret"
    snap_dir = tmp_path / "snapshots"

    env = os.environ.copy()
    env["BITSWAN_GITOPS_DIR"] = str(gitops_dir)
    env["BITSWAN_GITOPS_SECRET"] = secret
    env["SNAPSHOT_DIR"] = str(snap_dir)
    env["SNAPSHOT_RETENTION_PER_STAGE"] = "5"
    env["BITSWAN_WORKSPACE_NAME"] = "test-workspace"
    env["BITSWAN_WORKSPACE_REPO_DIR"] = str(workspace_repo_dir)
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
            "warning",
        ],
        cwd=str(os.path.join(os.path.dirname(__file__), "..")),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    base_url = f"http://127.0.0.1:{port}"
    headers = {"Authorization": f"Bearer {secret}"}

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
        pytest.fail(f"Server did not start within 30s.\nOutput:\n{output}")

    yield base_url, headers, snap_dir

    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------


class TestSnapshotAuth:
    def test_post_without_bearer_returns_401(self, server):
        base_url, _, _ = server
        r = httpx.post(
            f"{base_url}/snapshots",
            json={"source_stage": "dev"},
            timeout=5,
        )
        assert r.status_code == 401

    def test_get_without_bearer_returns_401(self, server):
        base_url, _, _ = server
        r = httpx.get(f"{base_url}/snapshots", timeout=5)
        assert r.status_code == 401


class TestSnapshotCreateList:
    def test_create_and_list(self, server):
        """POST /snapshots → task completes → GET /snapshots lists it."""
        base_url, headers, snap_dir = server

        r = httpx.post(
            f"{base_url}/snapshots",
            headers=headers,
            json={"source_stage": "dev", "name": "e2e-test"},
            timeout=10,
        )
        assert r.status_code == 202
        task_id = r.json()["task_id"]

        task = _poll_task(base_url, headers, task_id)
        assert task["status"] == "success", f"Task failed: {task.get('error')}"
        assert task["step"] == "done"

        # GET /snapshots lists the new snapshot
        r = httpx.get(f"{base_url}/snapshots", headers=headers, timeout=5)
        assert r.status_code == 200
        data = r.json()
        assert len(data["snapshots"]) >= 1

        snap = next(
            (s for s in data["snapshots"] if s.get("source_stage") == "dev"), None
        )
        assert snap is not None
        assert snap["name"] == "e2e-test"

        # Verify the 4 expected files are on disk
        snap_dir_path = snap_dir / snap["snapshot_id"]
        assert snap_dir_path.exists()
        for fname in (
            "postgres.tar.gz",
            "couchdb.tar.gz",
            "minio.tar.gz",
            "manifest.json",
        ):
            assert (snap_dir_path / fname).exists(), f"Missing {fname}"

        # manifest has the right fields
        manifest = json.loads((snap_dir_path / "manifest.json").read_text())
        assert manifest["source_stage"] == "dev"
        assert manifest["workspace"] == "test-workspace"
        assert "sizes_bytes" in manifest
        assert "known_limitations" in manifest

    def test_get_single_snapshot(self, server):
        """GET /snapshots/{id} returns the manifest."""
        base_url, headers, snap_dir = server

        r = httpx.get(f"{base_url}/snapshots", headers=headers, timeout=5)
        snaps = r.json()["snapshots"]
        if not snaps:
            pytest.skip("No snapshots yet (run test_create_and_list first)")

        snap_id = snaps[0]["snapshot_id"]
        r = httpx.get(f"{base_url}/snapshots/{snap_id}", headers=headers, timeout=5)
        assert r.status_code == 200
        assert r.json()["snapshot_id"] == snap_id

    def test_get_nonexistent_snapshot_returns_404(self, server):
        base_url, headers, _ = server
        r = httpx.get(
            f"{base_url}/snapshots/does-not-exist", headers=headers, timeout=5
        )
        assert r.status_code == 404


class TestSnapshotDelete:
    def test_delete_snapshot(self, server):
        """DELETE /snapshots/{id} → 202, file removed from disk."""
        base_url, headers, snap_dir = server

        # Create one to delete
        r = httpx.post(
            f"{base_url}/snapshots",
            headers=headers,
            json={"source_stage": "staging"},
            timeout=10,
        )
        assert r.status_code == 202
        task_id = r.json()["task_id"]
        _poll_task(base_url, headers, task_id)

        r = httpx.get(f"{base_url}/snapshots", headers=headers, timeout=5)
        staging_snaps = [
            s for s in r.json()["snapshots"] if s["source_stage"] == "staging"
        ]
        assert staging_snaps
        snap_id = staging_snaps[0]["snapshot_id"]

        r = httpx.delete(f"{base_url}/snapshots/{snap_id}", headers=headers, timeout=5)
        assert r.status_code == 202
        del_task_id = r.json()["task_id"]
        del_task = _poll_task(base_url, headers, del_task_id)
        assert del_task["status"] == "success"

        # No longer on disk
        assert not (snap_dir / snap_id).exists()

    def test_delete_nonexistent_returns_404(self, server):
        base_url, headers, _ = server
        r = httpx.delete(f"{base_url}/snapshots/ghost-id", headers=headers, timeout=5)
        assert r.status_code == 404


class TestSnapshotProductionGuard:
    def test_clone_into_production_without_flag_returns_400(self, server):
        """Clone into production without confirm flag should be refused."""
        base_url, headers, _ = server

        # First create a snapshot to clone from
        r = httpx.post(
            f"{base_url}/snapshots",
            headers=headers,
            json={"source_stage": "dev"},
            timeout=10,
        )
        assert r.status_code == 202
        task_id = r.json()["task_id"]
        task = _poll_task(base_url, headers, task_id)
        assert task["status"] == "success"

        r = httpx.get(f"{base_url}/snapshots", headers=headers, timeout=5)
        snaps = [s for s in r.json()["snapshots"] if s["source_stage"] == "dev"]
        snap_id = snaps[0]["snapshot_id"]

        r = httpx.post(
            f"{base_url}/snapshots/{snap_id}/clone",
            headers=headers,
            json={"target_stage": "production"},
            timeout=5,
        )
        assert r.status_code == 400
        assert "confirm" in r.json()["detail"].lower()

    def test_clone_into_production_with_flag_accepted(self, server):
        """Clone into production with confirm flag should be accepted (202)."""
        base_url, headers, _ = server

        r = httpx.get(f"{base_url}/snapshots", headers=headers, timeout=5)
        snaps = [s for s in r.json()["snapshots"] if s["source_stage"] == "dev"]
        if not snaps:
            pytest.skip("No dev snapshots available")

        snap_id = snaps[0]["snapshot_id"]

        r = httpx.post(
            f"{base_url}/snapshots/{snap_id}/clone",
            headers=headers,
            json={
                "target_stage": "production",
                "confirm_destination_is_production": True,
            },
            timeout=5,
        )
        assert r.status_code == 202


class TestSnapshotRetention:
    def test_retention_keeps_n_newest(self, server):
        """Creating snapshots beyond retention limit auto-deletes oldest."""
        base_url, headers, snap_dir = server

        # Temporarily override retention in env isn't possible at runtime,
        # so we test via the HTTP API using the live retention of 5 and
        # create enough snapshots of "staging" to exceed it.
        # Create 6 staging snapshots (retention=5 → 5 remain after last create)
        for i in range(6):
            r = httpx.post(
                f"{base_url}/snapshots",
                headers=headers,
                json={"source_stage": "staging", "name": f"retention-{i}"},
                timeout=15,
            )
            assert r.status_code == 202
            task_id = r.json()["task_id"]
            task = _poll_task(base_url, headers, task_id, timeout=30)
            assert (
                task["status"] == "success"
            ), f"Create {i} failed: {task.get('error')}"

        r = httpx.get(f"{base_url}/snapshots", headers=headers, timeout=5)
        staging_snaps = [
            s for s in r.json()["snapshots"] if s["source_stage"] == "staging"
        ]
        # With retention=5, should have at most 5 staging snapshots
        assert (
            len(staging_snaps) <= 5
        ), f"Expected at most 5 staging snapshots after retention, got {len(staging_snaps)}"


class TestSnapshotDiskPreflight:
    def test_disk_preflight_with_insufficient_space(self, tmp_path):
        """
        Unit-test the disk pre-flight logic by patching shutil.disk_usage.
        Operates directly on SnapshotService without starting the server.
        """
        import asyncio
        from unittest.mock import patch

        from app.services.snapshot_service import SnapshotService
        from app.snapshot_manager import SnapshotManager

        manager = SnapshotManager()
        snap_dir = tmp_path / "snapshots"
        svc = SnapshotService(
            workspace_name="test-ws",
            snapshot_manager=manager,
            snapshots_dir=snap_dir,
            retention_per_stage=5,
        )

        DiskUsage = type("DiskUsage", (), {"free": 100})  # only 100 bytes free

        with patch("shutil.disk_usage", return_value=DiskUsage()):
            # estimate_size returns 0 (services not enabled), so needed = 0*1.2+1 = 1
            # 1 > 100 is False — so disk check passes with 0-byte estimate.
            # To trigger the 507, we need estimate > free/1.2.
            # Patch estimate_size to return large values.
            async def _run():
                async def big_estimate(stage):
                    return {
                        "postgres": 1000,
                        "couchdb": 1000,
                        "minio": 1000,
                        "total": 3000,
                    }

                svc.estimate_size = big_estimate
                task_id = await svc.create_snapshot("dev")
                # Wait for the background task to finish
                await asyncio.sleep(1.0)
                task = manager.get_task(task_id)
                return task

            task = asyncio.run(_run())

        assert task is not None
        assert task.status == "error"
        assert "disk" in task.error.lower() or "space" in task.error.lower()

        # No partial dir should remain
        partial_dirs = list(snap_dir.glob("*.partial")) if snap_dir.exists() else []
        # Note: partial dir IS left for forensics on failure — but the error
        # happens before PREPARING_DIR so no partial dir is created yet.
        assert len(partial_dirs) == 0


class TestSnapshotEstimate:
    def test_estimate_returns_dict(self, server):
        """GET /snapshots/estimate?stage=dev returns a sizes dict."""
        base_url, headers, _ = server
        r = httpx.get(
            f"{base_url}/snapshots/estimate",
            headers=headers,
            params={"stage": "dev"},
            timeout=10,
        )
        assert r.status_code == 200
        data = r.json()
        assert "postgres" in data
        assert "couchdb" in data
        assert "minio" in data
        assert "total" in data
        assert isinstance(data["total"], int)


class TestWorktreeSnapshot:
    """Validates the per-worktree snapshot API surface.

    The fixture's workspace_repo_dir contains worktrees `wt-a` and `wt-b`
    but no Postgres container, so create-snapshot tasks finish in `error`
    (postgres "is not enabled"). The tests assert on the routing/validation
    layer and the manifest shape captured before the dump attempt.
    """

    def test_create_unknown_worktree_returns_400(self, server):
        base_url, headers, _ = server
        r = httpx.post(
            f"{base_url}/snapshots",
            headers=headers,
            json={"source_stage": "dev", "worktree": "ghost"},
            timeout=5,
        )
        assert r.status_code == 400
        assert "ghost" in r.json()["detail"].lower()

    def test_create_worktree_snapshot_routes_to_worktree_path(self, server):
        """POST /snapshots with worktree=wt-a → task starts with a 'wt-' id,
        records worktree on the task, then ends in error because postgres
        isn't running in CI."""
        base_url, headers, _ = server
        r = httpx.post(
            f"{base_url}/snapshots",
            headers=headers,
            json={"source_stage": "dev", "worktree": "wt-a", "name": "before-x"},
            timeout=10,
        )
        assert r.status_code == 202
        task_id = r.json()["task_id"]

        task = _poll_task(base_url, headers, task_id)
        assert task["worktree"] == "wt-a"
        assert task["snapshot_id"].startswith("wt-wt-a-")
        # Postgres unavailable in CI → expect graceful failure
        assert task["status"] == "error"
        assert "not enabled" in (task["error"] or "").lower()

    def test_estimate_for_worktree(self, server):
        """GET /snapshots/estimate?stage=dev&worktree=wt-a returns postgres-only
        sizes; couchdb/minio are 0."""
        base_url, headers, _ = server
        r = httpx.get(
            f"{base_url}/snapshots/estimate",
            headers=headers,
            params={"stage": "dev", "worktree": "wt-a"},
            timeout=10,
        )
        assert r.status_code == 200
        data = r.json()
        assert data["couchdb"] == 0
        assert data["minio"] == 0
        assert data["total"] == data["postgres"]

    def test_estimate_unknown_worktree_returns_400(self, server):
        base_url, headers, _ = server
        r = httpx.get(
            f"{base_url}/snapshots/estimate",
            headers=headers,
            params={"stage": "dev", "worktree": "ghost"},
            timeout=5,
        )
        assert r.status_code == 400

    def test_clone_requires_exactly_one_target(self, server):
        """target_stage AND target_worktree → 400; neither → 400."""
        base_url, headers, _ = server

        # Need an existing snapshot id to address the endpoint; the existing
        # dev snapshot tests created one earlier in the session.
        r = httpx.get(f"{base_url}/snapshots", headers=headers, timeout=5)
        snaps = r.json()["snapshots"]
        if not snaps:
            pytest.skip("No snapshots available")
        snap_id = snaps[0]["snapshot_id"]

        # Both set → 400
        r = httpx.post(
            f"{base_url}/snapshots/{snap_id}/clone",
            headers=headers,
            json={"target_stage": "dev", "target_worktree": "wt-a"},
            timeout=5,
        )
        assert r.status_code == 400

        # Neither set → 400
        r = httpx.post(
            f"{base_url}/snapshots/{snap_id}/clone",
            headers=headers,
            json={},
            timeout=5,
        )
        assert r.status_code == 400

    def test_clone_stage_snapshot_with_target_worktree_returns_400(self, server):
        """A stage-kind snapshot must not be cloned via target_worktree."""
        base_url, headers, _ = server

        # Find a stage-kind snapshot from earlier tests
        r = httpx.get(f"{base_url}/snapshots", headers=headers, timeout=5)
        snaps = [
            s for s in r.json()["snapshots"] if s.get("source_kind", "stage") == "stage"
        ]
        if not snaps:
            pytest.skip("No stage snapshots available")
        snap_id = snaps[0]["snapshot_id"]

        r = httpx.post(
            f"{base_url}/snapshots/{snap_id}/clone",
            headers=headers,
            json={"target_worktree": "wt-a"},
            timeout=5,
        )
        assert r.status_code == 400
        assert "worktree" in r.json()["detail"].lower()

    def test_clone_worktree_snapshot_unknown_target_returns_400(self, server):
        """Create a synthetic worktree-kind snapshot on disk and try to clone
        it into a non-existent worktree."""
        base_url, headers, snap_dir = server

        # Synthesize a worktree-kind snapshot directory + manifest. We can't
        # rely on the create endpoint because postgres isn't running.
        synth_id = "wt-wt-a-20260101T000000Z-deadbeef"
        sdir = snap_dir / synth_id
        sdir.mkdir(parents=True, exist_ok=True)
        (sdir / "postgres.tar.gz").write_bytes(b"")
        manifest = {
            "snapshot_id": synth_id,
            "name": "synthetic",
            "source_kind": "worktree",
            "source_stage": "dev",
            "worktree": "wt-a",
            "db_name": "postgres_wt_wt_a",
            "workspace": "test-workspace",
            "created_at": "2026-01-01T00:00:00+00:00",
            "sizes_bytes": {"postgres": 0, "total": 0},
            "gitops_version": "0.1.0",
        }
        (sdir / "manifest.json").write_text(json.dumps(manifest))

        r = httpx.post(
            f"{base_url}/snapshots/{synth_id}/clone",
            headers=headers,
            json={"target_worktree": "ghost"},
            timeout=5,
        )
        assert r.status_code == 400
        assert "ghost" in r.json()["detail"].lower()

    def test_clone_worktree_snapshot_with_target_stage_returns_400(self, server):
        """A worktree-kind snapshot must not be cloned via target_stage."""
        base_url, headers, snap_dir = server

        # Reuse the synthetic snapshot from the previous test if present,
        # otherwise synthesise one.
        synth_id = "wt-wt-a-20260101T000000Z-deadbeef"
        sdir = snap_dir / synth_id
        if not sdir.exists():
            sdir.mkdir(parents=True)
            (sdir / "postgres.tar.gz").write_bytes(b"")
            (sdir / "manifest.json").write_text(
                json.dumps(
                    {
                        "snapshot_id": synth_id,
                        "name": "synthetic",
                        "source_kind": "worktree",
                        "source_stage": "dev",
                        "worktree": "wt-a",
                        "db_name": "postgres_wt_wt_a",
                        "workspace": "test-workspace",
                        "created_at": "2026-01-01T00:00:00+00:00",
                        "sizes_bytes": {"postgres": 0, "total": 0},
                        "gitops_version": "0.1.0",
                    }
                )
            )

        r = httpx.post(
            f"{base_url}/snapshots/{synth_id}/clone",
            headers=headers,
            json={"target_stage": "dev"},
            timeout=5,
        )
        assert r.status_code == 400
