"""Phase 3 tests: expose_to is gone; the single knob is `expose: true|false`.

Exposed automations register an endpoint on the bailey via the daemon's
/ingress/add-route API. The bailey then handles OIDC + MFA + per-endpoint
ACL in front of the container — no per-app oauth2-proxy sidecar, no per-app
Keycloak client.
"""

import inspect
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
APP_DIR = REPO_ROOT / "app"


def test_get_expose_to_for_stage_removed():
    """The per-stage expose_to resolver shouldn't exist anymore."""
    import app.utils as utils

    assert not hasattr(utils, "get_expose_to_for_stage"), (
        "get_expose_to_for_stage was re-introduced. expose_to is dead — "
        "single knob is `expose: true|false`."
    )


def test_automation_config_has_no_expose_to_fields():
    """AutomationConfig used to carry dev/staging/production_expose_to lists.
    All three should be gone now."""
    from app.utils import AutomationConfig

    fields = (
        set(AutomationConfig.__dataclass_fields__.keys())
        if hasattr(AutomationConfig, "__dataclass_fields__")
        else set(vars(AutomationConfig).keys())
    )
    forbidden = {"dev_expose_to", "staging_expose_to", "production_expose_to"}
    leaked = fields & forbidden
    assert not leaked, f"AutomationConfig still carries expose_to fields: {leaked}"


def test_no_expose_to_substring_in_app():
    """Meta-test: nothing in app/ should mention expose_to anymore."""
    offenders = []
    for p in APP_DIR.rglob("*.py"):
        try:
            text = p.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        if "expose_to" in text:
            offenders.append(str(p.relative_to(REPO_ROOT)))
    assert not offenders, "expose_to references remain in app/: " + ", ".join(offenders)


def test_get_or_create_automation_client_removed():
    """Per-automation Keycloak clients are gone — bailey-proxy has one shared
    client for the whole server."""
    from app.services.automation_service import AutomationService

    assert not hasattr(AutomationService, "get_or_create_automation_client"), (
        "get_or_create_automation_client was re-introduced. Per-automation "
        "Keycloak clients are no longer used."
    )


def test_automation_service_doesnt_set_oauth2_proxy_env():
    """The OAUTH2_PROXY_* env-var injection block is gone. Grep the source as
    proof — if it ever comes back, this test fails first."""
    src = (APP_DIR / "services" / "automation_service.py").read_text()
    assert "OAUTH2_PROXY_CLIENT_ID" not in src, (
        "automation_service.py is setting OAUTH2_PROXY_CLIENT_ID — that's "
        "the dead expose_to sidecar path."
    )
    assert "OAUTH2_PROXY_CLIENT_SECRET" not in src, (
        "automation_service.py is setting OAUTH2_PROXY_CLIENT_SECRET."
    )
    assert "OAUTH2_PROXY_ALLOWED_GROUPS" not in src, (
        "automation_service.py is setting OAUTH2_PROXY_ALLOWED_GROUPS."
    )


def test_expose_branch_routes_directly_to_app_port():
    """When `expose: true` the deployer should call add_workspace_route_to_ingress
    with the APP's port, not an oauth2-proxy port. Inspect the compose-generator
    method source for the route call."""
    from app.services.automation_service import AutomationService

    # Find the method that generates docker compose entries (it's the one with
    # the `expose and port` branch).
    src = inspect.getsource(AutomationService)
    # The simple-expose call should be add_workspace_route_to_ingress(
    # dep_automation_name, dep_context, dep_stage, port)
    # — with `port` not `self.oauth2_proxy_port`.
    assert "self.oauth2_proxy_port" not in src, (
        "automation_service still references self.oauth2_proxy_port — "
        "the simple-expose branch should pass `port` directly."
    )
