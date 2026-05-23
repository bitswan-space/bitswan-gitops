# Plan: rip out oauth2-proxy from gitops + dashboard, route automations through bailey

## Context

`bitswan-automation-server` now has the **bailey** iframe-proxy chain:

```
cloud → platform-traefik → bitswan-protected-proxy (oauth2-proxy, single instance)
      → daemon MFA gate (TOTP + device + per-endpoint ACL + chrome wrap)
      → workspace_traefik → service container
```

Every protected hostname enters through one OIDC layer + one MFA + ACL layer.
This is the **only** authentication path now. Per-app oauth2-proxy sidecars are
redundant.

In parallel, the `bitswan-workspace-dashboard` is taking over from the
code-server editor as the primary internal-app UX. The editor is being
deprecated.

## Target state

### Config model

Drop `expose_to` everywhere. The single knob is **`expose: true|false`**.

| Old config                              | New behaviour                                         |
| --------------------------------------- | ----------------------------------------------------- |
| `expose: false` (default)               | Service stays on the internal docker network only.    |
| `expose: true` + `expose_to: [groups]`  | Was: oauth2-proxy sidecar + per-app Keycloak client.  |
| `expose: true`                          | Now: register endpoint on the bailey; default-private (owner-only); add grants on the bailey share UI. |

Every exposed app shows up automatically in `/bailey/workspaces` → workspace
section → app cards.

### Component changes

| Repo                              | What changes                                                                |
| --------------------------------- | --------------------------------------------------------------------------- |
| `bitswan-gitops`                  | Drop the oauth2-proxy injection helpers, the per-app Keycloak client creation, and the `expose_to_groups` config branch. Calls daemon's `/ingress/add-route` with `expose=true` to register the endpoint on bailey. |
| `bitswan-workspace-dashboard`     | Remove oauth2-proxy from the Dockerfile + entrypoint. Trust `X-Forwarded-Email` / `X-Forwarded-Groups` set by bailey. Add tests around header trust. |
| `bitswan-automation-server`      | Editor service definitions deprecated; workspace init brings up dashboard as the workspace's UI (the editor was the user-facing app, dashboard takes that role). |

## Step-by-step

### Phase 1 — Survey (this turn)
- [x] Clone both repos
- [x] Map every `oauth2-proxy` injection site
- [x] Map every `expose_to` usage
- [x] Write up this plan
- [ ] Push the plan to a branch in each repo for review

### Phase 2 — Strip oauth2-proxy from gitops (Python)
- Remove `app/services/oauth2_helpers.py` (whole file).
- In `app/services/automation_service.py`:
  - Drop `start_oauth2_proxy_in_container` + `start_oauth2_proxy_in_infra_services`.
  - Drop the imports + callsites (~10 spots).
  - Drop the per-automation Keycloak client creation (`get_or_create_oauth_client_for_automation` / similar).
- In `app/services/infra_service.py`: same.
- In `app/services/postgres_service.py`, `minio_service.py`, `deploy_manager.py`: same.
- Dockerfile: drop the `wget oauth2-proxy` line + binary install.
- **Tests** in `tests/`:
  - `test_deploy_no_oauth2_proxy.py` — deploy fixture asserts no oauth2-proxy in image / no `9999` listener.
  - `test_no_oauth2_imports.py` — meta-test: `grep -r oauth2 app/` returns empty.

### Phase 3 — Replace `expose_to` with `expose` in gitops
- `app/services/automation_service.py`:
  - Remove `expose_to_groups` parsing and `get_expose_to_for_stage`.
  - `expose: true` → call automation-server-daemon's `/ingress/add-route` with the automation hostname.
  - Owner-grant defaults to deployer's email (from `X-Forwarded-Email` on the gitops API request, passed through to the daemon call).
- Migration: any existing automation config with `expose_to` falls back to `expose: true` and surfaces a warning.
- **Tests**:
  - `test_expose_registers_endpoint.py` — mock daemon, assert `/ingress/add-route` was called with the right hostname + owner.
  - `test_expose_false_no_endpoint.py` — `expose: false` doesn't call the daemon.
  - `test_legacy_expose_to_warns.py` — old configs still deploy but emit a deprecation warning.

### Phase 4 — Strip oauth2-proxy from dashboard (TypeScript)
- `server/src/routes/auth.ts`:
  - Drop the oauth2-proxy callback handling.
  - Keep just: `GET /auth/whoami` returning `{email, groups}` parsed from `X-Forwarded-Email` / `X-Forwarded-Groups`.
- `server/src/routes/coding-agent.ts`: drop oauth2 refs.
- `client/index.html`: drop any oauth login link.
- `Dockerfile`: drop oauth2-proxy binary install.
- `entrypoint.sh`: drop oauth2-proxy startup.
- **Tests** in `server/test/`:
  - `auth.test.ts` — whoami returns email/groups when headers set; returns 401 when not.
  - `header-trust.test.ts` — requests with forged X-Forwarded-Email but coming from outside `bitswan_network` are dropped (the proxy enforces this, but the dashboard's tests should at least verify it doesn't blindly trust unauthenticated origins).

### Phase 5 — Editor removal
- `bitswan-automation-server/internal/daemon/workspace_init.go`: drop the editor service block (`*-editor.<domain>` registration, editor container start).
- `bitswan-automation-server/internal/services/editor.go`: deprecate (return error on Enable).
- Replace with dashboard service bring-up.
- **Tests**:
  - `internal/daemon/workspace_init_test.go` — workspace init with no editor flag still succeeds; dashboard is registered as the workspace's primary app.

### Phase 6 — End-to-end integration
- New workspace via bailey:
  1. User clicks "+ New workspace"
  2. Daemon creates gitops + dashboard containers (no editor)
  3. Dashboard endpoint registered with `expose: true`
- User deploys an automation through the dashboard:
  1. Dashboard pushes config to gitops
  2. Gitops deploys container with `expose: true` in config
  3. Gitops calls `/ingress/add-route` on the daemon
  4. New endpoint visible in `/bailey/workspaces` immediately
- **Tests**:
  - `e2e_deploy_via_dashboard.py` — full flow with mocked OIDC.

### Phase 7 — PRs
One PR per repo:
- `bitswan-gitops`: branch `feat/bailey-protected-ingress`
- `bitswan-workspace-dashboard`: branch `feat/bailey-protected-ingress`
- `bitswan-automation-server`: already on `feat/protected-ingress`, additional commits

## Test strategy summary

| Repo                          | Test framework | Target count |
| ----------------------------- | -------------- | ------------ |
| bitswan-gitops                | pytest         | ~15-20       |
| bitswan-workspace-dashboard   | vitest / jest  | ~10-15       |
| bitswan-automation-server     | go test        | ~5-10 new    |

Every config-change site gets at least one test asserting the new behaviour and
one asserting the old behaviour still doesn't accidentally re-emerge (meta test
that greps the codebase).

## Open questions

1. Do existing deployed automations with `expose_to` configs need to be
   re-deployed by hand, or should the bailey have a one-shot migration that
   re-registers endpoints from gitops state? (Probably the latter — write it
   as a startup hook on the daemon side similar to `migrateInnerHostRoutes`.)
2. Dashboard's coding-agent feature: it had oauth2 hooks for some external
   service integration. Need to verify those aren't actual Keycloak — likely
   third-party (OpenAI, Anthropic) and unrelated.
