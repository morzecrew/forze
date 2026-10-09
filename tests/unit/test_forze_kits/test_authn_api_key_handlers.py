"""Self-service API-key kit handlers: issue / list / revoke.

Each handler resolves the *current* identity (401 if none) and delegates to the
``ApiKeyLifecyclePort``; these pin the mapping to/from the kit DTOs.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from forze.application.contracts.authn import (
    ApiKeyCredentials,
    ApiKeyInfo,
    AuthnIdentity,
    CredentialLifetime,
    IssuedApiKey,
)
from forze.base.exceptions import exc
from forze_kits.aggregates.authn import (
    AuthnApiKeyListDTO,
    AuthnIssueApiKeyRequestDTO,
    AuthnIssuedApiKeyDTO,
    AuthnPrincipalRefDTO,
    AuthnRevokeApiKeyRequestDTO,
)
from forze_kits.aggregates.authn.handlers import (
    AuthnIssueApiKey,
    AuthnListApiKeys,
    AuthnListPrincipalApiKeys,
    AuthnRevokeApiKey,
    AuthnRevokePrincipalApiKey,
)

pytestmark = pytest.mark.unit

_USER = AuthnIdentity(principal_id=uuid4())
_NOW = datetime(2026, 6, 13, tzinfo=UTC)


class _FakeApiKeyLifecycle:
    def __init__(self, *, keys: list[ApiKeyInfo] | None = None) -> None:
        self.issue_args: tuple | None = None
        self.revoked: list[str] = []
        self.revoked_by_admin: list[str] = []
        self._keys = keys or []

    async def issue_api_key(self, identity, *, actor_principal_id=None, label=None):
        self.issue_args = (identity, actor_principal_id, label)
        return IssuedApiKey(
            key=ApiKeyCredentials(key="raw-secret", prefix="sk"),
            key_id="kid-1",
            hint="raw-…cret",
            label=label,
            lifetime=CredentialLifetime(expires_at=_NOW),
        )

    async def list_api_keys(self, identity):
        return self._keys

    async def revoke_api_key(self, identity, key_id):
        self.revoked.append(key_id)

    async def refresh_api_key(self, credentials): ...  # pragma: no cover

    async def revoke_many_api_keys(self, identity, key_ids): ...  # pragma: no cover

    async def revoke_principal_api_key(self, key_id):
        self.revoked_by_admin.append(key_id)

    async def list_principal_api_keys(self, principal_id):
        self.listed_for = principal_id
        return self._keys


def _resolver(identity):
    return lambda: identity


# ....................... #


class TestIssue:
    @pytest.mark.asyncio
    async def test_maps_issued_key_to_dto_and_passes_metadata(self) -> None:
        agent = uuid4()
        port = _FakeApiKeyLifecycle()
        handler = AuthnIssueApiKey(resolver=_resolver(_USER), api_key_lifecycle=port)

        dto = await handler(
            AuthnIssueApiKeyRequestDTO(label="ChatGPT", actor_principal_id=agent)
        )

        assert isinstance(dto, AuthnIssuedApiKeyDTO)
        assert dto.api_key == "raw-secret"  # secret, returned once
        assert dto.key_id == "kid-1"
        assert dto.prefix == "sk"
        assert dto.hint == "raw-…cret"
        assert dto.label == "ChatGPT"
        assert dto.expires_at == _NOW
        # The handler forwarded the agent + label to the port.
        assert port.issue_args == (_USER, agent, "ChatGPT")

    @pytest.mark.asyncio
    async def test_no_identity_is_401(self) -> None:
        handler = AuthnIssueApiKey(
            resolver=_resolver(None), api_key_lifecycle=_FakeApiKeyLifecycle()
        )

        with pytest.raises(exc, match="Authentication required"):
            await handler(AuthnIssueApiKeyRequestDTO())


class TestList:
    @pytest.mark.asyncio
    async def test_maps_infos_to_non_secret_items(self) -> None:
        info = ApiKeyInfo(
            key_id=uuid4(),
            hint="ab…yz",
            label="Claude",
            actor_principal_id=uuid4(),
            prefix="sk",
            is_active=True,
            created_at=_NOW,
            expires_at=None,
        )
        handler = AuthnListApiKeys(
            resolver=_resolver(_USER),
            api_key_lifecycle=_FakeApiKeyLifecycle(keys=[info]),
        )

        dto = await handler(None)

        assert isinstance(dto, AuthnApiKeyListDTO)
        assert len(dto.keys) == 1
        item = dto.keys[0]
        assert item.key_id == info.key_id
        assert item.hint == "ab…yz"
        assert item.label == "Claude"
        assert item.is_active is True
        # No secret/hash surface exists on the item.
        assert not hasattr(item, "key_hash")
        assert not hasattr(item, "api_key")

    @pytest.mark.asyncio
    async def test_no_identity_is_401(self) -> None:
        handler = AuthnListApiKeys(
            resolver=_resolver(None), api_key_lifecycle=_FakeApiKeyLifecycle()
        )

        with pytest.raises(exc, match="Authentication required"):
            await handler(None)


class TestRevoke:
    @pytest.mark.asyncio
    async def test_revokes_by_id(self) -> None:
        key_id = uuid4()
        port = _FakeApiKeyLifecycle()
        handler = AuthnRevokeApiKey(resolver=_resolver(_USER), api_key_lifecycle=port)

        await handler(AuthnRevokeApiKeyRequestDTO(id=key_id))

        assert port.revoked == [str(key_id)]

    @pytest.mark.asyncio
    async def test_no_identity_is_401(self) -> None:
        handler = AuthnRevokeApiKey(
            resolver=_resolver(None), api_key_lifecycle=_FakeApiKeyLifecycle()
        )

        with pytest.raises(exc, match="Authentication required"):
            await handler(AuthnRevokeApiKeyRequestDTO(id=uuid4()))


class TestAdminList:
    @pytest.mark.asyncio
    async def test_lists_another_principals_keys(self) -> None:
        other = uuid4()
        info = ApiKeyInfo(
            key_id=uuid4(), hint="ab…yz", is_active=True, created_at=_NOW, expires_at=None
        )
        port = _FakeApiKeyLifecycle(keys=[info])
        handler = AuthnListPrincipalApiKeys(resolver=_resolver(_USER), api_key_lifecycle=port)

        out = await handler(AuthnPrincipalRefDTO(id=other))

        assert port.listed_for == other
        assert [item.key_id for item in out.keys] == [info.key_id]

    @pytest.mark.asyncio
    async def test_no_identity_is_401_before_the_port(self) -> None:
        port = _FakeApiKeyLifecycle()
        handler = AuthnListPrincipalApiKeys(resolver=_resolver(None), api_key_lifecycle=port)

        with pytest.raises(exc, match="Authentication required"):
            await handler(AuthnPrincipalRefDTO(id=uuid4()))

        assert not hasattr(port, "listed_for")


class TestAdminRevoke:
    @pytest.mark.asyncio
    async def test_revokes_by_id_without_an_owner(self) -> None:
        key_id = uuid4()
        port = _FakeApiKeyLifecycle()
        handler = AuthnRevokePrincipalApiKey(resolver=_resolver(_USER), api_key_lifecycle=port)

        await handler(AuthnRevokeApiKeyRequestDTO(id=key_id))

        assert port.revoked_by_admin == [str(key_id)] and port.revoked == []

    @pytest.mark.asyncio
    async def test_no_identity_is_401_before_the_port(self) -> None:
        # Registered in an app's own registry without guards, it still refuses anyone anonymous.
        port = _FakeApiKeyLifecycle()
        handler = AuthnRevokePrincipalApiKey(resolver=_resolver(None), api_key_lifecycle=port)

        with pytest.raises(exc, match="Authentication required"):
            await handler(AuthnRevokeApiKeyRequestDTO(id=uuid4()))

        assert port.revoked_by_admin == []
