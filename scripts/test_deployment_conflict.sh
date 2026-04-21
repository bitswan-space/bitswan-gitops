#!/usr/bin/env bash
# test_deployment_conflict.sh
#
# Validates the two conflict-detection scenarios for deployment name collisions.
#
# Scenario A: redeploying an automation we own — should succeed (container is
#             removed and recreated, no 409).
# Scenario B: a foreign container occupies the target name — should return 409
#             with a clear message.
#
# Usage:
#   ./scripts/test_deployment_conflict.sh [GITOPS_URL] [GITOPS_SECRET] [WORKSPACE]
#
# Defaults to the test1 workspace running locally.

set -euo pipefail

GITOPS_URL="${1:-http://localhost:8079}"
GITOPS_SECRET="${2:-${BITSWAN_GITOPS_SECRET:-}}"
WORKSPACE="${3:-test1}"

if [[ -z "$GITOPS_SECRET" ]]; then
    echo "ERROR: GITOPS_SECRET not set. Pass it as \$2 or export BITSWAN_GITOPS_SECRET."
    exit 1
fi

AUTH_HEADER="Authorization: Bearer $GITOPS_SECRET"

# Find a real automation source in the workspace to use as the test subject.
# We pick the first one returned by the agent deployments endpoint.
echo ""
echo "=== Fetching available automations in workspace '$WORKSPACE' ==="
SOURCES=$(curl -sf "$GITOPS_URL/agent/deployments?worktree=" \
    -H "$AUTH_HEADER" 2>/dev/null || echo "[]")

if [[ "$SOURCES" == "[]" || -z "$SOURCES" ]]; then
    echo "No automations found via /agent/deployments — scanning workspace dir instead."
    WORKSPACE_DIR="${BITSWAN_WORKSPACE_REPO_DIR:-/home/john/.config/bitswan/workspaces/$WORKSPACE/workspace}"
    TOML_FILE=$(find "$WORKSPACE_DIR" -name "automation.toml" -not -path "*/worktrees/*" | head -1)
    if [[ -z "$TOML_FILE" ]]; then
        echo "ERROR: No automation.toml found in $WORKSPACE_DIR. Cannot run test."
        exit 1
    fi
    REL_PATH=$(dirname "$(realpath --relative-to="$WORKSPACE_DIR" "$TOML_FILE")")
    echo "Using automation at relative path: $REL_PATH"
else
    REL_PATH=$(echo "$SOURCES" | python3 -c "import sys,json; s=json.load(sys.stdin); print(s[0]['relative_path'])" 2>/dev/null || echo "")
    if [[ -z "$REL_PATH" ]]; then
        echo "ERROR: Could not parse relative_path from sources."
        exit 1
    fi
    echo "Using automation: $REL_PATH"
fi

PASS=0
FAIL=0

check() {
    local desc="$1" expected_status="$2" actual_status="$3" body="$4"
    if [[ "$actual_status" == "$expected_status" ]]; then
        echo "  PASS [$actual_status] $desc"
        ((PASS++)) || true
    else
        echo "  FAIL [$actual_status != $expected_status] $desc"
        echo "       body: $body"
        ((FAIL++)) || true
    fi
}

# ── Scenario A: redeployment (same deployment_id, our container) ──────────────
echo ""
echo "=== Scenario A: redeployment of our own automation ==="
echo "    Deploying '$REL_PATH' twice — second deploy should succeed (200/202)."

deploy_body() {
    python3 -c "import json; print(json.dumps({'relative_path': '$REL_PATH'}))"
}

RESP1=$(curl -sf -o /dev/null -w "%{http_code}" -X POST "$GITOPS_URL/automations/start-live-dev" \
    -H "$AUTH_HEADER" -H "Content-Type: application/json" \
    -d "$(deploy_body)" 2>/dev/null || echo "000")
echo "  First deploy: HTTP $RESP1"

# Give the first deploy a moment to register the container name.
sleep 3

RESP2_BODY=$(curl -s -X POST "$GITOPS_URL/automations/start-live-dev" \
    -H "$AUTH_HEADER" -H "Content-Type: application/json" \
    -d "$(deploy_body)" 2>/dev/null || echo "{}")
RESP2=$(curl -sf -o /dev/null -w "%{http_code}" -X POST "$GITOPS_URL/automations/start-live-dev" \
    -H "$AUTH_HEADER" -H "Content-Type: application/json" \
    -d "$(deploy_body)" 2>/dev/null || echo "000")

check "Second deploy of same automation succeeds (no 409)" "202" "$RESP2" "$RESP2_BODY"

# ── Scenario B: foreign container occupying the name ─────────────────────────
echo ""
echo "=== Scenario B: foreign container blocking the deployment name ==="

# Derive the container name gitops would use.
# Format: {workspace}-{sanitized_source_name}-{stage}
SOURCE_NAME=$(basename "$REL_PATH")
SANITIZED=$(echo "$SOURCE_NAME" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9-]/-/g' | sed 's/^-//;s/-$//')
CONTAINER_NAME="${WORKSPACE}-${SANITIZED}-live-dev"
echo "    Target container name: $CONTAINER_NAME"

# Create a foreign container with that name (busybox, keeps running briefly).
echo "    Spawning foreign container '$CONTAINER_NAME'..."
docker run -d --name "$CONTAINER_NAME" busybox sh -c "sleep 30" >/dev/null 2>&1 || {
    echo "    (container already exists — removing and re-creating)"
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1
    docker run -d --name "$CONTAINER_NAME" busybox sh -c "sleep 30" >/dev/null 2>&1
}

RESP3_BODY=$(curl -s -X POST "$GITOPS_URL/automations/start-live-dev" \
    -H "$AUTH_HEADER" -H "Content-Type: application/json" \
    -d "$(deploy_body)" 2>/dev/null || echo "{}")
RESP3=$(curl -sf -o /dev/null -w "%{http_code}" -X POST "$GITOPS_URL/automations/start-live-dev" \
    -H "$AUTH_HEADER" -H "Content-Type: application/json" \
    -d "$(deploy_body)" 2>/dev/null || echo "000")

check "Foreign container → 409 conflict" "409" "$RESP3" "$RESP3_BODY"

# Check the message is useful.
if echo "$RESP3_BODY" | grep -q "already exists"; then
    echo "  PASS error message contains 'already exists'"
    ((PASS++)) || true
else
    echo "  FAIL error message missing 'already exists': $RESP3_BODY"
    ((FAIL++)) || true
fi

# Clean up the foreign container.
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
echo "    Foreign container removed."

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[[ $FAIL -eq 0 ]]
