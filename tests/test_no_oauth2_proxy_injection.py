"""Meta-test that guards the architectural decision: per-container
oauth2-proxy sidecars are gone. bailey-proxy in front of the workspace
traefik handles authentication; gitops just registers endpoints on the
bailey and lets the chain do its job.

If you're reading this because the test failed: don't add back
copy_oauth2_proxy_to_container, the oauth2-proxy binary install in
the Dockerfile, or `gitops.oauth2.enabled` labels. Talk to the bailey
team about why the new chain isn't covering your use case first.
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
APP_DIR = REPO_ROOT / "app"
DOCKERFILE = REPO_ROOT / "Dockerfile"

FORBIDDEN_SUBSTRINGS = [
    "copy_oauth2_proxy_to_container",
    "is_oauth2_proxy_running",
    "/usr/local/bin/oauth2-proxy",
    "bitswan-aoc-oauth2/releases",
]

ALLOWED_FILES = {
    "tests/test_no_oauth2_proxy_injection.py",   # this file itself
    "PLAN-bailey-protected-ingress.md",          # plan doc
}


def _iter_repo_text_files():
    for p in REPO_ROOT.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(REPO_ROOT).as_posix()
        if rel.startswith((".git/", "node_modules/", ".venv/", "__pycache__/")):
            continue
        if rel in ALLOWED_FILES:
            continue
        if p.suffix in {".pyc", ".lock"}:
            continue
        try:
            yield rel, p.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue


def test_oauth2_helpers_module_deleted():
    assert not (APP_DIR / "services" / "oauth2_helpers.py").exists(), (
        "app/services/oauth2_helpers.py was reintroduced. Don't re-add the "
        "oauth2-proxy sidecar pattern; bailey-proxy is the auth layer now."
    )


def test_dockerfile_doesnt_install_oauth2_proxy():
    if not DOCKERFILE.exists():
        return
    text = DOCKERFILE.read_text(encoding="utf-8")
    assert "oauth2-proxy" not in text.lower(), (
        "Dockerfile installs oauth2-proxy. Auth is bailey-proxy's job now."
    )


def test_no_oauth2_proxy_injection_helpers_referenced():
    offenders = []
    for rel, text in _iter_repo_text_files():
        for needle in FORBIDDEN_SUBSTRINGS:
            if needle in text:
                offenders.append(f"{rel}: contains {needle!r}")
    assert not offenders, "Forbidden oauth2-proxy hooks reintroduced:\n  " + "\n  ".join(offenders)


def test_oauth2_enabled_returns_false():
    """The InfraService.oauth2_enabled property short-circuits all the
    per-container oauth2-proxy code paths. It must keep returning False
    until those code paths are deleted."""
    from app.services.infra_service import InfraService
    instance = InfraService.__new__(InfraService)
    assert instance.oauth2_enabled is False
