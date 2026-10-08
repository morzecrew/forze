"""A delegated caller cannot manage the credentials of the principal it acts for.

An agent acting for a user holds the intersection of the two. Minting an API key for the user
would hand it a credential that authenticates as the user alone, with no actor, and that escapes
the intersection for good; revoking the user's keys or sessions, or changing the password, acts
on the user's own standing. Switching tenant mints a token the same way, and leaving a tenant
drops the user's membership. Each self-service handler refuses a delegated identity.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.base.exceptions import CoreException, ExceptionKind
from forze_kits.aggregates.authn import (
    AuthnChangePasswordRequestDTO,
    AuthnIssueApiKeyRequestDTO,
    AuthnRevokeApiKeyRequestDTO,
)
from forze_kits.aggregates.authn.handlers import (
    AuthnChangePassword,
    AuthnIssueApiKey,
    AuthnListApiKeys,
    AuthnLogout,
    AuthnRevokeApiKey,
    AuthnRevokePrincipalApiKey,
)
from forze_kits.aggregates.tenancy import (
    LeaveTenant,
    SwitchTenant,
    TenantLeaveRequestDTO,
    TenantSwitchRequestDTO,
)

pytestmark = pytest.mark.unit

USER = AuthnIdentity(principal_id=uuid4())
AS_AGENT_FOR_USER = AuthnIdentity(
    principal_id=USER.principal_id, actor=AuthnIdentity(principal_id=uuid4())
)


class _Recorder:
    """Every lifecycle port at once, recording what reached it."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> Any:
        async def _call(*args: Any, **kwargs: Any) -> Any:
            self.calls.append(name)
            return []

        return _call


def _handlers(port: _Recorder) -> dict[str, tuple[Any, Any]]:
    resolve = lambda: AS_AGENT_FOR_USER  # noqa: E731

    return {
        "issue": (
            AuthnIssueApiKey(resolver=resolve, api_key_lifecycle=port),  # type: ignore[arg-type]
            AuthnIssueApiKeyRequestDTO(),
        ),
        "revoke": (
            AuthnRevokeApiKey(resolver=resolve, api_key_lifecycle=port),  # type: ignore[arg-type]
            AuthnRevokeApiKeyRequestDTO(id=uuid4()),
        ),
        # An administrator's act is never taken by an agent acting for the administrator.
        "admin-revoke": (
            AuthnRevokePrincipalApiKey(resolver=resolve, api_key_lifecycle=port),  # type: ignore[arg-type]
            AuthnRevokeApiKeyRequestDTO(id=uuid4()),
        ),
        "logout": (
            AuthnLogout(resolver=resolve, token_lifecycle=port),  # type: ignore[arg-type]
            None,
        ),
        "change-password": (
            AuthnChangePassword(resolver=resolve, password_lifecycle=port),  # type: ignore[arg-type]
            AuthnChangePasswordRequestDTO(
                current_password="old-secret", new_password="new-secret-1"
            ),
        ),
        "switch-tenant": (
            SwitchTenant(resolver=resolve, tenant_resolver=port, token_lifecycle=port),  # type: ignore[arg-type]
            TenantSwitchRequestDTO(id=uuid4()),
        ),
        "leave-tenant": (
            LeaveTenant(resolver=resolve, tenant_management=port),  # type: ignore[arg-type]
            TenantLeaveRequestDTO(id=uuid4()),
        ),
    }


@pytest.mark.parametrize(
    "name",
    [
        "issue",
        "revoke",
        "admin-revoke",
        "logout",
        "change-password",
        "switch-tenant",
        "leave-tenant",
    ],
)
async def test_a_delegated_caller_is_refused_before_the_port(name: str) -> None:
    port = _Recorder()
    handler, args = _handlers(port)[name]

    with pytest.raises(CoreException) as caught:
        await handler(args)

    assert caught.value.kind is ExceptionKind.AUTHORIZATION
    assert caught.value.code == "delegate_denied"
    assert port.calls == []


async def test_listing_keys_stays_open_to_a_delegated_caller() -> None:
    # Reading non-secret descriptors gives the agent nothing the user's standing did not.
    port = _Recorder()

    await AuthnListApiKeys(resolver=lambda: AS_AGENT_FOR_USER, api_key_lifecycle=port)(None)  # type: ignore[arg-type]

    assert port.calls == ["list_api_keys"]
