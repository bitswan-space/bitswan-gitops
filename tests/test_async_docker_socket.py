"""The async Docker client must honour $DOCKER_HOST so gitops talks to the
per-workspace container-manager socket instead of the host's
/var/run/docker.sock (which is no longer mounted into the container).

This pins the env-aware resolver. A regression that re-hard-codes
/var/run/docker.sock fails this test.
"""

from app.async_docker import AsyncDockerClient, _default_socket_path


def test_default_socket_path_unset_falls_back(monkeypatch):
    monkeypatch.delenv("DOCKER_HOST", raising=False)
    assert _default_socket_path() == "/var/run/docker.sock"


def test_default_socket_path_empty_falls_back(monkeypatch):
    monkeypatch.setenv("DOCKER_HOST", "")
    assert _default_socket_path() == "/var/run/docker.sock"


def test_default_socket_path_unix_uri(monkeypatch):
    monkeypatch.setenv(
        "DOCKER_HOST", "unix:///var/run/bitswan-cm-myws/container-manager.sock"
    )
    assert _default_socket_path() == "/var/run/bitswan-cm-myws/container-manager.sock"


def test_default_socket_path_non_unix_falls_back(monkeypatch):
    """gitops never talks Docker over TCP — but if someone exports a tcp://
    DOCKER_HOST, the async client should fall back rather than try to use it
    as a literal path."""
    monkeypatch.setenv("DOCKER_HOST", "tcp://docker.example.com:2375")
    assert _default_socket_path() == "/var/run/docker.sock"


def test_async_docker_client_uses_env_socket(monkeypatch):
    """The class constructor must read the env var when no explicit path is
    passed — the previous default was hard-coded /var/run/docker.sock."""
    monkeypatch.setenv("DOCKER_HOST", "unix:///tmp/proxied.sock")
    client = AsyncDockerClient()
    assert client.socket_path == "/tmp/proxied.sock"


def test_async_docker_client_explicit_path_wins(monkeypatch):
    """An explicit socket_path overrides $DOCKER_HOST so tests + callers
    that want a specific path can pin it."""
    monkeypatch.setenv("DOCKER_HOST", "unix:///should/be/ignored.sock")
    client = AsyncDockerClient(socket_path="/explicit/path.sock")
    assert client.socket_path == "/explicit/path.sock"
