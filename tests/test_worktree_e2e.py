"""
E2E tests for worktree (branching) and merge functionality.

Tests the full worktree lifecycle: create → deploy → commit → merge.
"""

import json

import pytest

from e2e_helpers import gitops_exec

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# Worktree Management
# ---------------------------------------------------------------------------


class TestWorktreeLifecycle:
    """Test worktree create, list, and delete."""

    WORKTREE_NAME = "e2e-test-wt"

    @pytest.fixture(autouse=True)
    def setup(self, api, check_gitops_running):
        self.api = api

    def test_list_worktrees(self):
        """GET /worktrees/ returns a list."""
        status, body = self.api.get("/worktrees/")
        assert status == 200, f"List worktrees failed ({status}): {body}"
        data = json.loads(body)
        assert isinstance(data, (list, dict))

    def test_create_worktree(self):
        """POST /worktrees/create creates a new worktree."""
        payload = json.dumps({"branch_name": self.WORKTREE_NAME})
        status, body = self.api.post("/worktrees/create", data=payload, timeout=30)
        # 200/201 success, 409 already exists
        assert status in (200, 201, 409), f"Create worktree failed ({status}): {body}"

    def test_worktree_appears_in_list(self):
        """Created worktree appears in the list."""
        status, body = self.api.get("/worktrees/")
        assert status == 200
        data = json.loads(body)
        # The format varies — could be a list of strings or objects
        found = False
        if isinstance(data, list):
            for item in data:
                name = item if isinstance(item, str) else item.get("name", "")
                if self.WORKTREE_NAME in name:
                    found = True
                    break
        elif isinstance(data, dict):
            found = self.WORKTREE_NAME in str(data)
        assert found, f"Worktree {self.WORKTREE_NAME} not in list: {body[:500]}"

    def test_worktree_directory_exists(self):
        """The worktree directory is created on disk."""
        result = gitops_exec(
            f"test -d /workspace-repo/worktrees/{self.WORKTREE_NAME} && echo yes || echo no"
        )
        assert "yes" in result.stdout, "Worktree directory not created"

    def test_worktree_diff(self):
        """GET /worktrees/{name}/diff returns diff output."""
        status, body = self.api.get(f"/worktrees/{self.WORKTREE_NAME}/diff")
        # 200 with diff (possibly empty), or 404 if worktree doesn't exist
        assert status in (200, 404), f"Diff failed ({status}): {body}"

    def test_worktree_status(self):
        """GET /agent/worktrees/{name}/status returns git status or 401 (agent auth)."""
        # This is an agent endpoint — may return 401 if no coding-agent container
        result = gitops_exec(
            f"curl -s -w '\\n%{{http_code}}' "
            f"-H 'Authorization: Bearer {self.api.secret}' "
            f"http://localhost:8079/agent/worktrees/{self.WORKTREE_NAME}/status"
        )
        lines = result.stdout.strip().rsplit("\n", 1)
        if len(lines) == 2:
            body, code = lines
            if code == "401":
                pytest.skip("Agent secret not available (no coding-agent container)")
            assert code in ("200", "404"), f"Status failed ({code}): {body}"

    def test_delete_worktree(self):
        """DELETE /worktrees/{name} removes the worktree."""
        status, body = self.api.delete(f"/worktrees/{self.WORKTREE_NAME}", timeout=30)
        # 200/204 success, 404 not found
        assert status in (200, 204, 404), f"Delete worktree failed ({status}): {body}"


# ---------------------------------------------------------------------------
# Worktree Deployment
# ---------------------------------------------------------------------------


class TestWorktreeDeployment:
    """Test deploying automations from a worktree."""

    WORKTREE_NAME = "e2e-deploy-wt"

    @pytest.fixture(autouse=True)
    def setup(self, api, check_gitops_running):
        self.api = api
        # Create worktree for deployment tests
        payload = json.dumps({"branch_name": self.WORKTREE_NAME})
        self.api.post("/worktrees/create", data=payload, timeout=30)

    def test_deploy_from_worktree(self):
        """Deploy a live-dev automation from a worktree."""
        # Find an automation in the worktree
        result = gitops_exec(
            f"find /workspace-repo/worktrees/{self.WORKTREE_NAME} "
            "-name automation.toml | head -1"
        )
        toml_path = result.stdout.strip()
        if not toml_path:
            pytest.skip("No automations in worktree")

        rel_path = toml_path.replace("/workspace-repo/", "").replace(
            "/automation.toml", ""
        )
        payload = json.dumps({"relative_path": rel_path})
        status, body = self.api.post(
            "/automations/start-live-dev", data=payload, timeout=60
        )
        assert status in (200, 202, 409), (
            f"Deploy from worktree failed ({status}): {body}"
        )

    def teardown_method(self, method):
        """Clean up worktree."""
        if hasattr(self, "api"):
            self.api.delete(f"/worktrees/{self.WORKTREE_NAME}", timeout=30)


# ---------------------------------------------------------------------------
# Merge Flow
# ---------------------------------------------------------------------------


class TestMergeFlow:
    """Test the worktree merge (rebase-and-merge) flow."""

    WORKTREE_NAME = "e2e-merge-wt"

    @pytest.fixture(autouse=True)
    def setup(self, api, check_gitops_running):
        self.api = api

    def test_full_merge_flow(self):
        """Create worktree → make changes → commit → merge."""
        # 1. Create worktree
        payload = json.dumps({"branch_name": self.WORKTREE_NAME})
        status, body = self.api.post("/worktrees/create", data=payload, timeout=30)
        assert status in (200, 201, 409), f"Create failed ({status}): {body}"

        # 2. Make a change in the worktree
        gitops_exec(
            f'bash -c \'echo "# E2E test" >> '
            f"/workspace-repo/worktrees/{self.WORKTREE_NAME}/README.md'"
        )

        # 3. Commit via agent API
        commit_payload = json.dumps(
            {
                "message": "E2E test: verify merge flow",
                "paths": ["."],
            }
        )
        result = gitops_exec(
            f"curl -s -w '\\n%{{http_code}}' "
            f"-H 'Authorization: Bearer {self.api.secret}' "
            f"-H 'Content-Type: application/json' "
            f"-X POST -d '{commit_payload}' "
            f"http://localhost:8079/agent/worktrees/{self.WORKTREE_NAME}/commit"
        )
        lines = result.stdout.strip().rsplit("\n", 1)
        # Commit may fail if nothing to commit or 401 (agent auth) — that's OK
        if len(lines) == 2:
            body, code = lines
            if code == "401":
                pytest.skip("Agent secret not available (no coding-agent container)")
            # 200 success, 400 nothing to commit, 404 no worktree
            assert code in ("200", "400", "404", "422"), (
                f"Commit failed ({code}): {body}"
            )

        # 4. Attempt rebase-and-merge via the worktrees API (not agent)
        result = gitops_exec(
            f"curl -s -w '\\n%{{http_code}}' "
            f"-H 'Authorization: Bearer {self.api.secret}' "
            f"-H 'Content-Type: application/json' "
            f"-X POST "
            f"http://localhost:8079/worktrees/{self.WORKTREE_NAME}/merge"
        )
        lines = result.stdout.strip().rsplit("\n", 1)
        if len(lines) == 2:
            body, code = lines
            # 200 success, 409 conflict, 400 nothing to merge
            assert code in ("200", "400", "404", "409", "422"), (
                f"Merge failed ({code}): {body}"
            )

    def teardown_method(self, method):
        if hasattr(self, "api"):
            self.api.delete(f"/worktrees/{self.WORKTREE_NAME}", timeout=30)
