"""Tests for :mod:`forze_identity.authn.adapters.api_key_lifecycle`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from forze.application.contracts.authn import ApiKeyCredentials, AuthnIdentity
from forze.base.exceptions import CoreException, exc
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


class TestApiKeyLifecycleAdapterIssueDelegation:
    @pytest.mark.asyncio
    async def test_issue_persists_actor_principal_id(self) -> None:
        pid, agent = uuid4(), uuid4()
        ak_cmd = _port()
        ak_cmd.create = AsyncMock(return_value=_created_key())

        adapter = _adapter(ak_cmd=ak_cmd)
        adapter.eligibility.require_authentication_allowed = AsyncMock()

        await adapter.issue_api_key(
            AuthnIdentity(principal_id=pid), actor_principal_id=agent
        )

        create_cmd = ak_cmd.create.await_args.args[0]
        assert create_cmd.principal_id == pid
        assert create_cmd.actor_principal_id == agent

    @pytest.mark.asyncio
    async def test_issue_without_agent_is_non_delegated(self) -> None:
        ak_cmd = _port()
        ak_cmd.create = AsyncMock(return_value=_created_key())

        adapter = _adapter(ak_cmd=ak_cmd)
        adapter.eligibility.require_authentication_allowed = AsyncMock()

        await adapter.issue_api_key(AuthnIdentity(principal_id=uuid4()))

        assert ak_cmd.create.await_args.args[0].actor_principal_id is None

    @pytest.mark.asyncio
    async def test_issue_refuses_a_delegated_identity(self) -> None:
        # The key would authenticate as the principal alone, with no actor.
        ak_cmd = _port()
        ak_cmd.create = AsyncMock(return_value=_created_key())

        adapter = _adapter(ak_cmd=ak_cmd)
        adapter.eligibility.require_authentication_allowed = AsyncMock()
        delegated = AuthnIdentity(principal_id=uuid4(), actor=AuthnIdentity(principal_id=uuid4()))

        with pytest.raises(CoreException) as caught:
            await adapter.issue_api_key(delegated)

        assert caught.value.code == "delegate_denied"
        ak_cmd.create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_issue_persists_label_and_a_nonsecret_hint(self) -> None:
        ak_cmd = _port()
        ak_cmd.create = AsyncMock(return_value=_created_key())

        adapter = _adapter(ak_cmd=ak_cmd)
        adapter.eligibility.require_authentication_allowed = AsyncMock()

        issued = await adapter.issue_api_key(
            AuthnIdentity(principal_id=uuid4()), label="ChatGPT"
        )

        create_cmd = ak_cmd.create.await_args.args[0]
        assert create_cmd.label == "ChatGPT"
        assert create_cmd.hint and "…" in create_cmd.hint
        # The hint never contains the whole secret.
        assert issued.key.key not in create_cmd.hint
        assert issued.label == "ChatGPT"
        assert issued.hint == create_cmd.hint


class TestApiKeyLifecycleAdapterList:
    @pytest.mark.asyncio
    async def test_list_returns_non_secret_infos(self) -> None:
        pid, agent = uuid4(), uuid4()
        now = datetime.now(tz=UTC)
        account = ReadApiKeyAccount(
            id=uuid4(),
            rev=1,
            created_at=now,
            last_update_at=now,
            principal_id=pid,
            actor_principal_id=agent,
            prefix="sk",
            hint="ab…yz",
            label="Claude",
            key_hash="secret-digest",
            is_active=True,
        )

        ak_qry = _port()
        ak_qry.find_many = AsyncMock(return_value=MagicMock(hits=[account]))

        adapter = _adapter(ak_qry=ak_qry)
        adapter.eligibility.require_authentication_allowed = AsyncMock()

        infos = await adapter.list_api_keys(AuthnIdentity(principal_id=pid))

        assert len(infos) == 1
        info = infos[0]
        assert info.key_id == account.id
        assert info.hint == "ab…yz"
        assert info.label == "Claude"
        assert info.actor_principal_id == agent
        # ApiKeyInfo has no field that could carry the secret/hash.
        assert not hasattr(info, "key_hash")

    @pytest.mark.asyncio
    async def test_refresh_preserves_the_delegation_agent(self) -> None:
        pid, agent = uuid4(), uuid4()
        svc = ApiKeyService(pepper=b"x" * 32, config=ApiKeyConfig())
        key = "raw-key"
        now = datetime.now(tz=UTC)
        account = ReadApiKeyAccount(
            id=uuid4(),
            rev=2,
            created_at=now,
            last_update_at=now,
            principal_id=pid,
            actor_principal_id=agent,
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

        await adapter.refresh_api_key(ApiKeyCredentials(key=key))

        # The rotated key keeps acting for the same agent.
        assert ak_cmd.create.await_args.args[0].actor_principal_id == agent


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

    def test_colon_prefix_rejected_at_config(self) -> None:
        # Ingress splits ``prefix:key`` on the first ':', so a ':' in the prefix
        # would corrupt the split.
        with pytest.raises(exc, match="prefix"):
            ApiKeyConfig(prefix="sk:live")

    def test_whitespace_prefix_rejected_on_generate_override(self) -> None:
        svc = ApiKeyService(pepper=b"x" * 32, config=ApiKeyConfig())

        with pytest.raises(exc, match="prefix"):
            svc.generate_key(prefix=" sk")


# ....................... #


def _eligible_except(*refused: object) -> AsyncMock:
    """An eligibility gate refusing *refused*, as the policy-principal gate does."""

    async def _check(principal_id: object) -> None:
        if principal_id in refused:
            raise exc.authentication("Principal not found")

    return AsyncMock(side_effect=_check)


class TestTheDelegationActorIsValidated:
    """A delegation key names its agent: it must be another principal authentication accepts.

    The same eligibility gate authentication runs on the actor, so a key cannot be minted
    for an actor it could never authenticate with, nor name the subject as its own agent.
    """

    @pytest.mark.asyncio
    async def test_the_subject_cannot_be_its_own_actor(self) -> None:
        pid = uuid4()
        ak_cmd = _port()
        ak_cmd.create = AsyncMock(return_value=_created_key())
        adapter = _adapter(ak_cmd=ak_cmd)
        adapter.eligibility.require_authentication_allowed = _eligible_except()

        with pytest.raises(CoreException) as caught:
            await adapter.issue_api_key(AuthnIdentity(principal_id=pid), actor_principal_id=pid)

        assert caught.value.kind.value == "validation"
        assert caught.value.code == "delegate_invalid"
        ak_cmd.create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_actor_authentication_would_refuse_is_refused(self) -> None:
        # Unknown and deactivated principals both fail the same gate.
        pid, agent = uuid4(), uuid4()
        ak_cmd = _port()
        ak_cmd.create = AsyncMock(return_value=_created_key())
        adapter = _adapter(ak_cmd=ak_cmd)
        adapter.eligibility.require_authentication_allowed = _eligible_except(agent)

        with pytest.raises(CoreException) as caught:
            await adapter.issue_api_key(AuthnIdentity(principal_id=pid), actor_principal_id=agent)

        assert caught.value.kind.value == "validation"
        assert caught.value.code == "delegate_invalid"
        ak_cmd.create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failure_checking_the_actor_is_not_masked(self) -> None:
        pid, agent = uuid4(), uuid4()
        adapter = _adapter()

        async def _check(principal_id: object) -> None:
            if principal_id == agent:
                raise exc.infrastructure("policy store down")

        adapter.eligibility.require_authentication_allowed = AsyncMock(side_effect=_check)

        with pytest.raises(CoreException) as caught:
            await adapter.issue_api_key(AuthnIdentity(principal_id=pid), actor_principal_id=agent)

        assert caught.value.kind.value == "infrastructure"

    @pytest.mark.asyncio
    async def test_refresh_refuses_an_actor_no_longer_eligible(self) -> None:
        pid, agent = uuid4(), uuid4()
        svc = ApiKeyService(pepper=b"x" * 32, config=ApiKeyConfig())
        key = "raw-key"
        now = datetime.now(tz=UTC)
        account = ReadApiKeyAccount(
            id=uuid4(),
            rev=2,
            created_at=now,
            last_update_at=now,
            principal_id=pid,
            actor_principal_id=agent,
            key_hash=svc.calculate_key_digest(key),
            is_active=True,
        )
        ak_qry = _port()
        ak_qry.find = AsyncMock(return_value=account)
        ak_cmd = _port()
        ak_cmd.create = AsyncMock(return_value=_created_key())
        ak_cmd.update = AsyncMock()
        adapter = _adapter(api_key_svc=svc, ak_qry=ak_qry, ak_cmd=ak_cmd)
        adapter.eligibility.require_authentication_allowed = _eligible_except(agent)

        with pytest.raises(CoreException) as caught:
            await adapter.refresh_api_key(ApiKeyCredentials(key=key))

        assert caught.value.code == "delegate_invalid"
        ak_cmd.create.assert_not_awaited()
        ak_cmd.update.assert_not_awaited()


# ....................... #


class _Registry:
    """A principal registry knowing some principals by kind."""

    def __init__(self, kinds: dict[object, str]) -> None:
        self.kinds = kinds

    async def get_principal(self, principal_id: object) -> object:
        from forze.application.contracts.authz import PrincipalRef

        kind = self.kinds.get(principal_id)

        if kind is None:
            return None

        if kind == "inactive-service":
            return PrincipalRef(principal_id=principal_id, kind="service", is_active=False)  # type: ignore[arg-type]

        return PrincipalRef(principal_id=principal_id, kind=kind)  # type: ignore[arg-type]


class TestTheDelegationAgentIsAService:
    """With the authz principal registry wired, a key's agent must be a service principal."""

    def _issuing(self, registry: _Registry) -> tuple[ApiKeyLifecycleAdapter, MagicMock]:
        ak_cmd = _port()
        ak_cmd.create = AsyncMock(return_value=_created_key())
        adapter = _adapter(ak_cmd=ak_cmd, principal_registry=registry)
        adapter.eligibility.require_authentication_allowed = _eligible_except()
        return adapter, ak_cmd

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "kind",
        ["user", None, "inactive-service"],
        # An inactive service passes an allow-all eligibility gate; the registry still refuses.
        ids=["a-user", "unregistered", "inactive-service"],
    )
    async def test_an_agent_that_is_not_a_service_is_refused(self, kind: str | None) -> None:
        pid, agent = uuid4(), uuid4()
        adapter, ak_cmd = self._issuing(_Registry({agent: kind} if kind else {}))

        with pytest.raises(CoreException) as caught:
            await adapter.issue_api_key(AuthnIdentity(principal_id=pid), actor_principal_id=agent)

        assert caught.value.code == "delegate_invalid"
        # One message for "not a service" and "not found": no principal enumeration.
        assert str(caught.value.summary) == (
            "The delegation agent must be another registered, active principal"
        )
        ak_cmd.create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_service_agent_is_accepted(self) -> None:
        pid, agent = uuid4(), uuid4()
        adapter, ak_cmd = self._issuing(_Registry({agent: "service"}))

        await adapter.issue_api_key(AuthnIdentity(principal_id=pid), actor_principal_id=agent)

        assert ak_cmd.create.await_args.args[0].actor_principal_id == agent

    @pytest.mark.asyncio
    async def test_refresh_refuses_an_agent_that_is_not_a_service(self) -> None:
        pid, agent = uuid4(), uuid4()
        svc = ApiKeyService(pepper=b"x" * 32, config=ApiKeyConfig())
        now = datetime.now(tz=UTC)
        account = ReadApiKeyAccount(
            id=uuid4(),
            rev=1,
            created_at=now,
            last_update_at=now,
            principal_id=pid,
            actor_principal_id=agent,
            key_hash=svc.calculate_key_digest("raw-key"),
            is_active=True,
        )
        ak_qry = _port()
        ak_qry.find = AsyncMock(return_value=account)
        ak_cmd = _port()
        ak_cmd.create = AsyncMock(return_value=_created_key())
        ak_cmd.update = AsyncMock()
        adapter = _adapter(
            api_key_svc=svc, ak_qry=ak_qry, ak_cmd=ak_cmd, principal_registry=_Registry({agent: "user"})
        )
        adapter.eligibility.require_authentication_allowed = _eligible_except()

        with pytest.raises(CoreException) as caught:
            await adapter.refresh_api_key(ApiKeyCredentials(key="raw-key"))

        assert caught.value.code == "delegate_invalid"
        ak_cmd.create.assert_not_awaited()


def test_the_wired_lifecycle_takes_the_registry_from_the_authz_route() -> None:
    from forze.application.contracts.authz import AuthzSpec
    from forze_identity.authn.execution.deps.deps import ConfigurableApiKeyLifecycle

    shared = MagicMock()
    ctx = MagicMock()
    registry = object()
    ctx.authz.principal_registry = MagicMock(return_value=registry)
    ctx.deps.provide = MagicMock(return_value=lambda _c, _s: MagicMock())

    with patch(
        "forze_identity.authn.execution.deps.deps.ApiKeyLifecycleAdapter"
    ) as adapter_cls:
        ConfigurableApiKeyLifecycle(shared=shared, authz_route="policy")(ctx, MagicMock())
        wired = adapter_cls.call_args.kwargs["principal_registry"]
        ConfigurableApiKeyLifecycle(shared=shared)(ctx, MagicMock())
        unwired = adapter_cls.call_args.kwargs["principal_registry"]

    assert wired is registry
    ctx.authz.principal_registry.assert_called_once_with(AuthzSpec(name="policy"))
    assert unwired is None
