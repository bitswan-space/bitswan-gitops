#!/bin/bash
# setup_test_env.sh — Provision a BitSwan test workspace on a remote host.
#
# This script sets up the test infrastructure needed to run the E2E test suite.
# It's idempotent — safe to run multiple times.
#
# Usage:
#   ./tests/setup_test_env.sh [SSH_HOST] [WORKSPACE_NAME]
#
# Example:
#   ./tests/setup_test_env.sh root@88.99.15.208 e2e-test
#
# What it does:
#   1. Builds the automation-server from source (if repo exists on host)
#   2. Starts the daemon with correct volume mounts
#   3. Initializes a workspace with VPN, stage networks, sub-Traefik
#   4. Starts the coding agent container
#   5. Deploys a test automation
#   6. Outputs the env vars needed to run pytest
#
# Prerequisites:
#   - SSH access to the test host
#   - Go 1.24+ on the host (at /usr/local/go/bin or in PATH)
#   - Docker with compose plugin on the host
#   - bitswan-automation-server repo at /root/bitswan-automation-server
#   - bitswan-gitops repo at /root/bitswan-gitops

set -euo pipefail

HOST="${1:-root@88.99.15.208}"
WORKSPACE="${2:-e2e-test}"
DOMAIN="${WORKSPACE}.bswn.io"

echo "=== BitSwan E2E Test Environment Setup ==="
echo "Host: $HOST"
echo "Workspace: $WORKSPACE"
echo "Domain: $DOMAIN"
echo ""

run() {
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$HOST" "$@"
}

# 1. Check prerequisites
echo "[1/8] Checking prerequisites..."
run "docker --version > /dev/null && echo 'Docker: OK'" || { echo "FAIL: Docker not available"; exit 1; }
run "ls /root/bitswan-automation-server/go.mod > /dev/null 2>&1 && echo 'Automation server repo: OK'" || { echo "FAIL: /root/bitswan-automation-server not found"; exit 1; }

# 2. Build automation-server
echo "[2/8] Building automation-server..."
run "cd /root/bitswan-automation-server && export PATH=/usr/local/go/bin:\$PATH && go build -o /tmp/bitswan-e2e . 2>&1" | tail -3
echo "Build: OK"

# 3. Start/update daemon
echo "[3/8] Starting daemon..."
run bash <<REMOTE
docker rm -f bitswan-automation-server-daemon 2>/dev/null || true
cp /tmp/bitswan-e2e /usr/local/bin/bitswan-e2e
docker run -d \
  --name bitswan-automation-server-daemon \
  --restart always \
  -e HOST_HOME=/root \
  -e BITSWAN_TRAEFIK_HOST=traefik:8080 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /root:/root \
  -v /var/run/bitswan:/var/run/bitswan \
  -v /usr/local/bin/bitswan-e2e:/usr/local/bin/bitswan:ro \
  --network bitswan_network \
  bitswan/automation-server-runtime:latest 2>&1 | tail -1
sleep 3
docker ps --filter name=bitswan-automation-server-daemon --format '{{.Names}}: {{.Status}}'
REMOTE

# 4. Initialize workspace
echo "[4/8] Initializing workspace..."
run bash <<REMOTE
docker exec bitswan-automation-server-daemon /usr/local/bin/bitswan init \
  --workspace $WORKSPACE \
  --domain $DOMAIN \
  --gitops-image bitswan/gitops:latest \
  --no-remove 2>&1 | tail -5 || echo "(init may have partial failures on existing workspace)"
REMOTE

# 5. Initialize VPN
echo "[5/8] Initializing VPN..."
IP=$(echo "$HOST" | cut -d@ -f2)
run "docker exec bitswan-automation-server-daemon /usr/local/bin/bitswan vpn init --endpoint $IP 2>&1 | tail -3"

# 6. Start workspace sub-Traefik
echo "[6/8] Starting workspace sub-Traefik..."
run bash <<REMOTE
TRAEFIK_PATH=/root/.config/bitswan/workspaces/$WORKSPACE/traefik
mkdir -p \$TRAEFIK_PATH
cat > \$TRAEFIK_PATH/traefik.yml <<'EOF'
entryPoints:
  web:
    address: ":80"
api:
  insecure: true
providers:
  rest:
    insecure: true
EOF
cat > \$TRAEFIK_PATH/docker-compose.yml <<EOF
services:
  traefik:
    image: traefik:v3.6
    restart: always
    container_name: ${WORKSPACE}__traefik
    networks:
      - bitswan_network
      - ${WORKSPACE}-dev
      - ${WORKSPACE}-staging
      - ${WORKSPACE}-production
    volumes:
      - \$TRAEFIK_PATH/traefik.yml:/etc/traefik/traefik.yml:z
networks:
  bitswan_network:
    external: true
  ${WORKSPACE}-dev:
    external: true
  ${WORKSPACE}-staging:
    external: true
  ${WORKSPACE}-production:
    external: true
EOF
cd \$TRAEFIK_PATH
docker compose up -d 2>&1 | tail -3
REMOTE

# 7. Create test automation and deploy
echo "[7/8] Creating test automation..."
run bash <<REMOTE
WORKSPACE_DIR=/root/.config/bitswan/workspaces/$WORKSPACE/workspace
mkdir -p \$WORKSPACE_DIR/E2ETest/hello-world
cat > \$WORKSPACE_DIR/E2ETest/hello-world/automation.toml <<'EOF'
[deployment]
image = "nginx:alpine"
expose = true
port = 80
EOF
cd \$WORKSPACE_DIR
git add -A 2>/dev/null
git -c user.email="e2e@test.com" -c user.name="E2E" commit -m "Add test automation" 2>/dev/null || true
REMOTE

# 8. Discover config and output env vars
echo "[8/8] Discovering configuration..."
GITOPS_CONTAINER=$(run "docker ps --filter name=${WORKSPACE}.*gitops --format '{{.Names}}' | head -1")
SECRET=$(run "docker inspect $GITOPS_CONTAINER --format '{{range .Config.Env}}{{println .}}{{end}}' | grep BITSWAN_GITOPS_SECRET | cut -d= -f2")

echo ""
echo "=== Test Environment Ready ==="
echo ""
echo "Run the E2E tests with:"
echo ""
echo "  export BITSWAN_TEST_HOST=$HOST"
echo "  export BITSWAN_TEST_WORKSPACE=$WORKSPACE"
echo "  export BITSWAN_TEST_CONTAINER=$GITOPS_CONTAINER"
echo "  export BITSWAN_TEST_SECRET=$SECRET"
echo "  python3 -m pytest tests/ -m e2e -v"
echo ""
