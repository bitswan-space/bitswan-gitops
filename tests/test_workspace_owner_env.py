"""When gitops deploys an exposed automation it must forward the
workspace owner email (BITSWAN_WORKSPACE_OWNER, set by the daemon's
`workspace init`) to the daemon's /ingress/add-route call. Otherwise
the bailey ACL row gets created with no owner and the app never
surfaces on the deployer's dashboard.
"""

import inspect


def test_expose_branch_reads_workspace_owner_env():
    """The expose:true branch in AutomationService must read
    BITSWAN_WORKSPACE_OWNER from os.environ. Inspect the source as
    proof — a regression that drops the env read fails this test."""
    from app.services.automation_service import AutomationService

    src = inspect.getsource(AutomationService)
    assert "BITSWAN_WORKSPACE_OWNER" in src, (
        "AutomationService no longer reads BITSWAN_WORKSPACE_OWNER — "
        "exposed automations will register with an empty owner and the "
        "bailey dashboard won't show them."
    )


def test_expose_branch_forwards_owner_to_add_route():
    """The same branch must thread the env value into
    add_workspace_route_to_ingress via owner_email=. The literal call
    should look like:

        add_workspace_route_to_ingress(
            dep_automation_name, dep_context, dep_stage, port,
            owner_email=workspace_owner,
        )
    """
    from app.services.automation_service import AutomationService

    src = inspect.getsource(AutomationService)
    assert "owner_email=workspace_owner" in src, (
        "AutomationService is no longer passing owner_email to "
        "add_workspace_route_to_ingress — the ACL row will be empty."
    )


def test_workspace_owner_falls_back_to_none_when_unset():
    """If BITSWAN_WORKSPACE_OWNER is unset/empty the env read should
    resolve to None (not ""), because add_workspace_route_to_ingress
    only forwards owner_email when truthy."""
    from app.services.automation_service import AutomationService

    src = inspect.getsource(AutomationService)
    # The pattern in the source is:
    #   workspace_owner = os.environ.get("BITSWAN_WORKSPACE_OWNER") or None
    # We just guard against the no-fallback ("...") variant that would
    # silently send the empty string downstream.
    assert 'os.environ.get("BITSWAN_WORKSPACE_OWNER") or None' in src, (
        "Empty-string fallback would cause the daemon to write empty "
        "owner rows. Use `... or None` so add_route_to_ingress omits "
        "the field instead."
    )
