"""Phase 6 tests: when gitops registers an exposed automation's route
on the daemon, it forwards the deployer's email + a friendly display
name. The daemon uses these to write the bailey ACL row so the
automation surfaces on the deployer's bailey workspaces page.

We don't talk to a real daemon here — mock the HTTP layer and assert
the request body shape.
"""

from unittest.mock import MagicMock

import app.utils as utils


def _mock_client_response(status=200):
    """Return (client_factory, captured_kwargs_list)."""
    captured: list[dict] = []
    response = MagicMock()
    response.status_code = status
    response.text = "ok"

    def factory():
        client = MagicMock()

        # Track the json= kwarg of every .post() call.
        def post(url, json=None, **kw):
            captured.append({"url": url, "body": json})
            return response

        client.post.side_effect = post
        client.__enter__ = MagicMock(return_value=client)
        client.__exit__ = MagicMock(return_value=False)
        return client, "http://daemon"

    return factory, captured


def test_add_route_to_ingress_includes_owner_email_when_set(monkeypatch):
    factory, captured = _mock_client_response()
    monkeypatch.setattr(utils, "_ingress_client_and_base", factory)

    ok = utils.add_route_to_ingress(
        hostname="foo.example.com",
        upstream="foo:80",
        workspace_name="ws1",
        owner_email="alice@example.com",
        display_name="My Foo",
    )
    assert ok is True
    assert len(captured) == 1
    body = captured[0]["body"]
    assert body["hostname"] == "foo.example.com"
    assert body["upstream"] == "foo:80"
    assert body["workspace_name"] == "ws1"
    assert body["owner_email"] == "alice@example.com"
    assert body["display_name"] == "My Foo"


def test_add_route_to_ingress_omits_owner_when_not_set(monkeypatch):
    factory, captured = _mock_client_response()
    monkeypatch.setattr(utils, "_ingress_client_and_base", factory)

    utils.add_route_to_ingress(
        hostname="bar.example.com",
        upstream="bar:80",
        workspace_name="ws2",
    )
    body = captured[0]["body"]
    # When no owner is supplied (e.g. internal server-boot routes) the
    # field is absent so the daemon falls back to its auto-claim path.
    assert "owner_email" not in body
    assert "display_name" not in body


def test_add_workspace_route_forwards_owner_email(monkeypatch):
    factory, captured = _mock_client_response()
    monkeypatch.setattr(utils, "_ingress_client_and_base", factory)
    monkeypatch.setenv("BITSWAN_GITOPS_DOMAIN", "example.com")
    monkeypatch.setenv("BITSWAN_WORKSPACE_NAME", "myws")

    utils.add_workspace_route_to_ingress(
        automation_name="auto",
        context="prod",
        stage="production",
        port="8080",
        owner_email="bob@example.com",
    )
    assert len(captured) == 1
    body = captured[0]["body"]
    assert body["owner_email"] == "bob@example.com"
    # display_name defaults to "<auto> (<stage>)" when not explicitly given.
    assert body["display_name"] == "auto (production)"


def test_add_workspace_route_lets_caller_override_display_name(monkeypatch):
    factory, captured = _mock_client_response()
    monkeypatch.setattr(utils, "_ingress_client_and_base", factory)

    utils.add_workspace_route_to_ingress(
        automation_name="auto",
        context="prod",
        stage="production",
        port="8080",
        owner_email="bob@example.com",
        display_name="Production analytics",
    )
    assert captured[0]["body"]["display_name"] == "Production analytics"
