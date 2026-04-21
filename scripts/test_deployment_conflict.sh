#!/usr/bin/env bash
# test_deployment_conflict.sh
#
# Validates the two conflict-detection scenarios for deployment name collisions.
#
# Scenario A: redeploying an automation we own — should succeed (202, no 409).
#             The route handler removes the existing container so compose can
#             recreate it cleanly.
# Scenario B: a foreign container (no gitops label) occupies the target name —
#             should return 409 with a message containing 'already exists'.
#
# Usage:
#   ./scripts/test_deployment_conflict.sh [GITOPS_URL] [GITOPS_SECRET] [WORKSPACE]
#
# GITOPS_URL defaults to auto-detecting the running gitops container's Docker IP
# when localhost:8079 is not published to the host (normal in the bitswan stack).

set -euo pipefail

WORKSPACE="${3:-test1}"

# ── URL auto-detection ────────────────────────────────────────────────────────
# The gitops container may not publish 8079 to the host (only internal Docker
# networking). Find it by container name pattern and use its internal IP.
_detect_url() {
    if curl -sf --connect-timeout 2 "http://localhost:8079/" -o /dev/null 2>/dev/null; then
        echo "http://localhost:8079"
        return
    fi
    local container
    container=$(docker ps --filter "name=${WORKSPACE}.*gitops" --format "{{.Names}}" 2>/dev/null | head -1)
    if [[ -n "$container" ]]; then
        local ip
        ip=$(docker inspect "$container" \
            --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{break}}{{end}}' 2>/dev/null)
        if [[ -n "$ip" ]]; then
            echo "http://${ip}:8079"
            return
        fi
    fi
    echo "http://localhost:8079"
}

GITOPS_URL="${1:-$(_detect_url)}"

# ── Secret ────────────────────────────────────────────────────────────────────
GITOPS_SECRET="${2:-${BITSWAN_GITOPS_SECRET:-}}"
if [[ -z "$GITOPS_SECRET" ]]; then
    # Last resort: read from the running gitops container's environment
    _container=$(docker ps --filter "name=${WORKSPACE}.*gitops" --format "{{.Names}}" 2>/dev/null | head -1)
    if [[ -n "$_container" ]]; then
        GITOPS_SECRET=$(docker exec "$_container" env 2>/dev/null \
            | grep '^BITSWAN_GITOPS_SECRET=' | cut -d= -f2 || true)
    fi
fi
if [[ -z "$GITOPS_SECRET" ]]; then
    echo "ERROR: GITOPS_SECRET not set. Pass as \$2 or export BITSWAN_GITOPS_SECRET."
    exit 1
fi

AUTH_HEADER="Authorization: Bearer $GITOPS_SECRET"
echo "Gitops URL : $GITOPS_URL"
echo "Workspace  : $WORKSPACE"

# ── curl helper: one round-trip captures both body and status ─────────────────
# Sets global RESP_BODY and RESP_STATUS.
http_post() {
    local url="$1" data="$2"
    local tmp
    tmp=$(mktemp)
    RESP_STATUS=$(curl -s -o "$tmp" -w "%{http_code}" -X POST "$url" \
        -H "$AUTH_HEADER" -H "Content-Type: application/json" \
        -d "$data" 2>/dev/null) || RESP_STATUS="000"
    RESP_BODY=$(cat "$tmp")
    rm -f "$tmp"
}

# ── Find automation ───────────────────────────────────────────────────────────
echo ""
echo "=== Fetching available automations in workspace '$WORKSPACE' ==="
SOURCES=$(curl -sf "$GITOPS_URL/agent/deployments?worktree=" \
    -H "$AUTH_HEADER" 2>/dev/null || echo "[]")

if [[ "$SOURCES" == "[]" || -z "$SOURCES" ]]; then
    echo "No automations via /agent/deployments — scanning workspace dir."
    WORKSPACE_DIR="${BITSWAN_WORKSPACE_REPO_DIR:-/home/john/.config/bitswan/workspaces/$WORKSPACE/workspace}"
    TOML_FILE=$(find "$WORKSPACE_DIR" -name "automation.toml" -not -path "*/worktrees/*" | head -1)
    if [[ -z "$TOML_FILE" ]]; then
        echo "ERROR: No automation.toml found in $WORKSPACE_DIR"
        exit 1
    fi
    REL_PATH=$(dirname "$(realpath --relative-to="$WORKSPACE_DIR" "$TOML_FILE")")
    DEP_ID=""
    echo "Using automation at: $REL_PATH"
else
    REL_PATH=$(echo "$SOURCES" | python3 -c \
        "import sys,json; s=json.load(sys.stdin); print(s[0]['relative_path'])" 2>/dev/null || echo "")
    DEP_ID=$(echo "$SOURCES" | python3 -c \
        "import sys,json; s=json.load(sys.stdin); print(s[0].get('deployment_id',''))" 2>/dev/null || echo "")
    [[ -z "$REL_PATH" ]] && { echo "ERROR: could not parse relative_path"; exit 1; }
    echo "Using automation: $REL_PATH  (deployment_id: $DEP_ID)"
fi

DEPLOY_BODY=$(python3 -c "import json; print(json.dumps({'relative_path': '$REL_PATH'}))")

PASS=0
FAIL=0

check() {
    local desc="$1" expected="$2" actual="$3" body="$4"
    if [[ "$actual" == "$expected" ]]; then
        echo "  PASS [$actual] $desc"
        ((PASS++)) || true
    else
        echo "  FAIL [$actual != $expected] $desc"
        echo "       body: $body"
        ((FAIL++)) || true
    fi
}

# ── wait for a container with the given label to appear ───────────────────────
wait_for_label() {
    local label="$1" timeout="${2:-25}"
    local i=0
    while (( i < timeout )); do
        docker ps --filter "label=$label" --format "{{.Names}}" 2>/dev/null | grep -q . && return 0
        sleep 1; ((i++))
    done
    return 1
}

# ── Scenario A ────────────────────────────────────────────────────────────────
echo ""
echo "=== Scenario A: redeployment of our own automation ==="
echo "    Two deploys of '$REL_PATH' — second should succeed (202)."

http_post "$GITOPS_URL/automations/start-live-dev" "$DEPLOY_BODY"
echo "  First deploy: HTTP $RESP_STATUS  body: $RESP_BODY"

# Wait for the first deploy's container to appear before firing the second.
if [[ -n "$DEP_ID" ]]; then
    echo "    Waiting for container (label gitops.deployment_id=$DEP_ID)..."
    wait_for_label "gitops.deployment_id=$DEP_ID" 25 || echo "    (container not seen — continuing anyway)"
else
    sleep 6
fi

http_post "$GITOPS_URL/automations/start-live-dev" "$DEPLOY_BODY"
check "Second deploy of same automation succeeds (no 409)" "202" "$RESP_STATUS" "$RESP_BODY"

# ── Scenario B ────────────────────────────────────────────────────────────────
echo ""
echo "=== Scenario B: foreign container blocking the deployment name ==="

# Get the actual container name via the gitops.deployment_id label — this accounts
# for the 4-char context hash that the simplified name derivation misses.
CONTAINER_NAME=""
if [[ -n "$DEP_ID" ]]; then
    CONTAINER_NAME=$(docker ps -a --filter "label=gitops.deployment_id=$DEP_ID" \
        --format "{{.Names}}" 2>/dev/null | head -1)
fi
if [[ -z "$CONTAINER_NAME" ]]; then
    # Fallback: sanitised name without hash (no context / simple automation)
    SANITIZED=$(basename "$REL_PATH" \
        | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9-]/-/g;s/^-//;s/-$//')
    CONTAINER_NAME="${WORKSPACE}-${SANITIZED}-live-dev"
    echo "  (label lookup failed — using derived name)"
fi
echo "  Target container name: $CONTAINER_NAME"

# Remove the real container (if any) and spawn a foreign one without gitops labels.
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
echo "  Spawning foreign container '$CONTAINER_NAME'..."
docker run -d --name "$CONTAINER_NAME" busybox sh -c "sleep 60" >/dev/null 2>&1

http_post "$GITOPS_URL/automations/start-live-dev" "$DEPLOY_BODY"
check "Foreign container → 409 conflict" "409" "$RESP_STATUS" "$RESP_BODY"

if echo "$RESP_BODY" | grep -q "already exists"; then
    echo "  PASS error message contains 'already exists'"
    ((PASS++)) || true
else
    echo "  FAIL error message missing 'already exists': $RESP_BODY"
    ((FAIL++)) || true
fi

docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
echo "  Foreign container removed."

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[[ $FAIL -eq 0 ]]
