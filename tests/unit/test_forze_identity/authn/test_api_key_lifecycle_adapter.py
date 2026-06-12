"""Tests for :mod:`forze_identity.authn.adapters.api_key_lifecycle`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from forze.application.contracts.authn import ApiKeyCredentials, AuthnIdentity
from forze.base.exceptions import exc
from forze_identity.authn.adapters.api_key_lifecycle import ApiKeyLifecycleAdapter
from forze_identity.authn.domain.models.account import ReadApiKeyAccount
from forze_identity.authn.services import ApiKeyConfig, ApiKeyService

pytestmark = pytest.mark.unit


def _port(*, cache=None, history_enabled: bool = False) -> MagicMock:
    port = MagicMock()
    port.spec = MagicMock(cache=cache, history_enabled=history_enabled)
    return port


def _adapter(**kwargs) -> ApiKeyLifecycleAdapter:
    defaults = {
        "api_key_svc": ApiKeyService(pepper=b"x" * 32, config=ApiKeyConfig()),
        "ak_qry": _port(),
        "ak_cmd": _port(),
        "eligibility": MagicMock(),
    }
    defaults.update(kwargs)
    return ApiKeyLifecycleAdapter(**defaults)


class TestApiKeyLifecycleAdapterInit:
    def test_rejects_query_cache(self) -> None:
        with pytest.raises(exc, match="caching"):
            _adapter(ak_qry=_port(cache={"route": True}))

    def test_rejects_command_cache(self) -> None:
        with pytest.raises(exc, match="caching"):
            _adapter(ak_cmd=_port(cache={"route": True}))

    def test_rejects_query_history(self) -> None:
        with pytest.raises(exc, match="history"):
            _adapter(ak_qry=_port(history_enabled=True))

    def test_rejects_command_history(self) -> None:
        with pytest.raises(exc, match="history"):
            _adapter(ak_cmd=_port(history_enabled=True))


class TestApiKeyLifecycleAdapterRevoke:
    @pytest.mark.asyncio
    async def test_revoke_invalid_key_id_raises_authentication(self) -> None:
        adapter = _adapter()
        adapter.eligibility.require_authentication_allowed = AsyncMock()

        with pytest.raises(exc, match="API key not found"):
            await adapter.revoke_api_key(
                AuthnIdentity(principal_id=uuid4()),
                "not-a-uuid",
            )

    @pytest.mark.asyncio
    async def test_revoke_deactivates_owned_key(self) -> None:
        pid = uuid4()
        key_id = uuid4()
        now = datetime.now(tz=UTC)
        account = ReadApiKeyAccount(
            id=key_id,
            rev=3,
            created_at=now,
            last_update_at=now,
            principal_id=pid,
            key_hash="h",
            is_active=True,
        )

        ak_qry = _port()
        ak_qry.find = AsyncMock(return_value=account)
        ak_cmd = _port()
        ak_cmd.update = AsyncMock()

        adapter = _adapter(ak_qry=ak_qry, ak_cmd=ak_cmd)
        adapter.eligibility.require_authentication_allowed = AsyncMock()

        await adapter.revoke_api_key(AuthnIdentity(principal_id=pid), str(key_id))

        ak_cmd.update.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_revoke_many_delegates_per_key(self) -> None:
        pid = uuid4()
        adapter = _adapter()
        adapter.eligibility.require_authentication_allowed = AsyncMock()

        ids = [str(uuid4()), str(uuid4())]
        with patch.object(
            ApiKeyLifecycleAdapter,
            "revoke_api_key",
            new_callable=AsyncMock,
        ) as revoke:
            await adapter.revoke_many_api_keys(AuthnIdentity(principal_id=pid), ids)

        assert revoke.await_count == 2


def _created_key() -> MagicMock:
    created = MagicMock()
    created.id = uuid4()
    created.created_at = datetime.now(tz=UTC)
    return created


class TestApiKeyLifecycleAdapterRefresh:
    @pytest.mark.asyncio
    async def test_refresh_rotates_active_key(self) -> None:
        pid = uuid4()
        svc = ApiKeyService(pepper=b"x" * 32, config=ApiKeyConfig())
        key = "raw-key"
        now = datetime.now(tz=UTC)
        account = ReadApiKeyAccount(
            id=uuid4(),
            rev=2,
            created_at=now,
            last_update_at=now,
            principal_id=pid,
            key_hash=svc.calculate_key_digest(key),
            is_active=True,
        )

        ak_qry = _port()
        ak_qry.find = AsyncMock(return_value=account)
        ak_cmd = _port()
        ak_cmd.create = AsyncMock(return_value=_created_key())
        ak_cmd.update = AsyncMock()

        adapter = _adapter(api_key_svc=svc, ak_qry=ak_qry, ak_cmd=ak_cmd)
        adapter.eligibility.require_authentication_allowed = AsyncMock()

        issued = await adapter.refresh_api_key(ApiKeyCredentials(key=key))

        # A fresh key is minted and the presented one is retired.
        assert issued.key.key != key
        ak_cmd.create.assert_awaited_once()
        ak_cmd.update.assert_awaited_once()
        update_cmd = ak_cmd.update.await_args.args[2]
        assert update_cmd.is_active is False

    @pytest.mark.asyncio
    async def test_refresh_rejects_unknown_key(self) -> None:
        ak_qry = _port()
        ak_qry.find = AsyncMock(return_value=None)

        adapter = _adapter(ak_qry=ak_qry)

        with pytest.raises(exc, match="API key not found"):
            await adapter.refresh_api_key(ApiKeyCredentials(key="nope"))

    @pytest.mark.asyncio
    async def test_refresh_rejects_inactive_key(self) -> None:
        now = datetime.now(tz=UTC)
        account = ReadApiKeyAccount(
            id=uuid4(),
            rev=1,
            created_at=now,
            last_update_at=now,
            principal_id=uuid4(),
            key_hash="h",
            is_active=False,
        )
        ak_qry = _port()
        ak_qry.find = AsyncMock(return_value=account)

        adapter = _adapter(ak_qry=ak_qry)

        with pytest.raises(exc, match="API key not found"):
            await adapter.refresh_api_key(ApiKeyCredentials(key="x"))

    @pytest.mark.asyncio
    async def test_refresh_rejects_expired_key(self) -> None:
        svc = ApiKeyService(pepper=b"x" * 32, config=ApiKeyConfig())
        key = "raw-key"
        now = datetime.now(tz=UTC)
        account = ReadApiKeyAccount(
            id=uuid4(),
            rev=1,
            created_at=now,
            last_update_at=now,
            principal_id=uuid4(),
            key_hash=svc.calculate_key_digest(key),
            is_active=True,
            expires_at=now - timedelta(seconds=1),
        )
        ak_qry = _port()
        ak_qry.find = AsyncMock(return_value=account)

        adapter = _adapter(api_key_svc=svc, ak_qry=ak_qry)

        with pytest.raises(exc, match="API key not found"):
            await adapter.refresh_api_key(ApiKeyCredentials(key=key))

    @pytest.mark.asyncio
    async def test_refresh_rejects_wrong_key(self) -> None:
        svc = ApiKeyService(pepper=b"x" * 32, config=ApiKeyConfig())
        now = datetime.now(tz=UTC)
        account = ReadApiKeyAccount(
            id=uuid4(),
            rev=1,
            created_at=now,
            last_update_at=now,
            principal_id=uuid4(),
            key_hash="mismatched-digest",
            is_active=True,
        )
        ak_qry = _port()
        ak_qry.find = AsyncMock(return_value=account)

        adapter = _adapter(api_key_svc=svc, ak_qry=ak_qry)
        adapter.eligibility.require_authentication_allowed = AsyncMock()

        with pytest.raises(exc, match="Invalid API key"):
            await adapter.refresh_api_key(ApiKeyCredentials(key="whatever"))


class TestApiKeyPrefixConfig:
    def test_configured_prefix_is_minted_into_keys(self) -> None:
        svc = ApiKeyService(pepper=b"x" * 32, config=ApiKeyConfig(prefix="sk"))

        res = svc.generate_key()

        assert isinstance(res, tuple)
        prefix, key = res
        assert prefix == "sk"
        assert key

    def test_empty_prefix_rejected_at_config(self) -> None:
        with pytest.raises(exc, match="prefix"):
            ApiKeyConfig(prefix="")

    def test_whitespace_prefix_rejected_at_config(self) -> None:
        with pytest.raises(exc, match="prefix"):
            ApiKeyConfig(prefix="sk live")

    def test_whitespace_prefix_rejected_on_generate_override(self) -> None:
        svc = ApiKeyService(pepper=b"x" * 32, config=ApiKeyConfig())

        with pytest.raises(exc, match="prefix"):
            svc.generate_key(prefix=" sk")
