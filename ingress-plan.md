# Ingress Authentication Plan

## Goal

Workspaces run gitops processes that need to add and remove routes on the shared
ingress daemon (bitswan-automation-server). Each workspace must only be able to
manage its own routes — it must not be able to add routes under another workspace's
hostname or delete another workspace's routes.

## Threat model

- A compromised or misbehaving workspace gitops process could attempt to:
  - Register a route for a hostname it does not own (hijacking traffic)
  - Delete routes belonging to another workspace (DoS)
  - Replay a stolen token to act as another workspace

## Proposed auth model: workspace-scoped JWT with shared secret

Each workspace is provisioned with a unique signing secret at workspace-init time.
The secret is stored as an env var (`BITSWAN_INGRESS_SECRET`) injected into the
gitops container. The automation server holds the same secret (or derives it from
a master key + workspace name using HMAC).

### Token structure

```json
{
  "workspace_name": "acme-prod",
  "exp": 1234567890,
  "iat": 1234567000
}
```

- `workspace_name` is the canonical workspace identifier
- Short expiry (e.g. 5 minutes) limits replay window
- Signed with HMAC-SHA256 using the workspace secret

### Request flow

1. Gitops generates a short-lived JWT signed with its secret, embedding its own
   workspace name.
2. Token is sent as `Authorization: Bearer <token>` (or the existing
   `BITSWAN_AUTOMATION_SERVER_DAEMON_TOKEN` header).
3. Ingress daemon verifies the signature using the workspace's secret.
4. Daemon extracts `workspace_name` from the verified token.
5. Daemon checks that the requested hostname matches `*.{workspace_name}.*` or
   the configured domain pattern for that workspace. Requests outside that
   namespace are rejected with 403.

### Secret provisioning options

**Option A — Per-workspace secret stored in automation server config**

- At workspace init, the automation server generates a random 256-bit secret and
  writes it to `~/.config/bitswan/workspaces/{name}/ingress.secret`.
- The same secret is passed to the gitops container as `BITSWAN_INGRESS_SECRET`.
- Rotation: re-init workspace and restart gitops container.
- Pros: simple, no external dependency.
- Cons: secret lives on disk; rotation requires container restart.

**Option B — Master key + HMAC derivation**

- The automation server has a single `BITSWAN_MASTER_SECRET`.
- Workspace secret = `HMAC-SHA256(master_secret, workspace_name)`.
- Gitops container receives `BITSWAN_INGRESS_SECRET` = derived value.
- Pros: single secret to manage on the server side; easy rotation by cycling the
  master key.
- Cons: a leaked workspace secret does not enable rotation without cycling master.

**Option C — AOC-issued tokens (future)**

- AOC (the control plane) issues short-lived tokens to each workspace on demand.
- The ingress daemon validates against AOC's public key (JWKS).
- Pros: centralised revocation, no shared secrets between daemon and workspace.
- Cons: requires AOC integration and network dependency at route-add time.

## Enforcement in the ingress daemon

```
POST /ingress/add-route
DELETE /ingress/remove-route/{hostname}
```

Both endpoints should:
1. Require a valid, non-expired JWT in the `Authorization` header.
2. Reject if `workspace_name` in the token does not prefix-match the hostname
   pattern for that workspace (e.g. hostname must contain the workspace name as
   a label component).
3. Log the workspace name and hostname for every add/remove for audit purposes.

## Hostname ownership check

Workspace `acme-prod` is authorised to manage routes whose hostname ends in
`.acme-prod.<domain>` or equals `acme-prod.<domain>`. A strict prefix rule:

```
allowed if: hostname == "{workspace_name}.{domain}"
         or hostname ends with ".{workspace_name}.{domain}"
         or hostname starts with "{workspace_name}-"  (for service subdomains)
```

The exact pattern should mirror how `generate_workspace_url` and `caddy_hostname`
construct hostnames in the gitops service.

## Rollout steps

1. Generate and provision `BITSWAN_INGRESS_SECRET` in workspace init (Go side).
2. Add JWT signing helper to bitswan-gitops (Python) — sign before every ingress
   call.
3. Add JWT verification + hostname ownership check to the ingress daemon handlers
   in bitswan-automation-server.
4. Gate enforcement behind a feature flag (`BITSWAN_INGRESS_AUTH_REQUIRED`) so
   existing deployments can migrate without a hard cutover.
5. Once all workspaces are provisioned with secrets, flip the flag to required.
