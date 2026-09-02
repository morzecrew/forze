"""Unit tests for authz operation-plan helpers."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import (
    AuthzDecision,
    AuthzDocumentScope,
    AuthzSpec,
)
from forze.application.contracts.base import CountlessPage
from forze.application.execution import Deps, InvocationMetadata
from forze.application.hooks.authz import (
    AuthzBeforeAuthorize,
    AuthzDocumentScopeWrap,
    merge_query_filters,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO
from tests.support.execution_context import (
    context_from_deps,
)

pytestmark = pytest.mark.unit

class _AllowDecision:
    async def authorize(self, request):
        _ = request
        return AuthzDecision(allowed=True, matched_permission_key="x.read")

class _DenyDecision:
    async def authorize(self, request):
        _ = request
        return AuthzDecision(allowed=False, reason="denied")

class _AllowExceptPrincipal:
    """Allow everyone except one principal id (to exercise actor/delegation checks)."""

    def __init__(self, denied) -> None:
        self.denied = denied

    async def authorize(self, request):
        if request.subject.principal_id == self.denied:
            return AuthzDecision(allowed=False, reason="actor not permitted")
        return AuthzDecision(allowed=True, matched_permission_key="x.read")

class _AllowDelegation:
    async def may_act(self, actor_id, subject_id, *, scope=None):
        _ = actor_id, subject_id, scope
        return True

class _DenyDelegation:
    async def may_act(self, actor_id, subject_id, *, scope=None):
        _ = actor_id, subject_id, scope
        return False

def test_merge_query_filters_and() -> None:
    merged = merge_query_filters({"$values": {"a": 1}}, {"$values": {"b": 2}})
    assert merged == {"$and": [{"$values": {"a": 1}}, {"$values": {"b": 2}}]}

@pytest.mark.asyncio
async def test_before_authorize_allows() -> None:
    ctx = context_from_deps(Deps())
    ident = AuthnIdentity(principal_id=uuid4())
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with patch.object(ctx.authz, "decision", return_value=_AllowDecision()):
        with ctx.inv_ctx.bind(metadata=metadata, authn=ident):
            hook = AuthzBeforeAuthorize(spec=AuthzSpec(name="z"), action="x.read")(ctx)
            await hook(None)

@pytest.mark.asyncio
async def test_before_authorize_denies() -> None:
    ctx = context_from_deps(Deps())
    ident = AuthnIdentity(principal_id=uuid4())
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with patch.object(ctx.authz, "decision", return_value=_DenyDecision()):
        with ctx.inv_ctx.bind(metadata=metadata, authn=ident):
            hook = AuthzBeforeAuthorize(spec=AuthzSpec(name="z"), action="x.read")(ctx)

            with pytest.raises(CoreException) as exc_info:
                await hook(None)
            assert exc_info.value.kind == ExceptionKind.AUTHORIZATION


@pytest.mark.asyncio
async def test_before_authorize_missing_identity_is_authentication() -> None:
    # No bound identity is a missing-credentials problem (401), not a denial (403).
    ctx = context_from_deps(Deps())
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with patch.object(ctx.authz, "decision", return_value=_AllowDecision()):
        with ctx.inv_ctx.bind(metadata=metadata):
            hook = AuthzBeforeAuthorize(spec=AuthzSpec(name="z"), action="x.read")(ctx)

            with pytest.raises(CoreException) as exc_info:
                await hook(None)
            assert exc_info.value.kind == ExceptionKind.AUTHENTICATION
            assert exc_info.value.code == "auth_required"


@pytest.mark.asyncio
async def test_document_scope_wrap_missing_identity_is_authentication() -> None:
    ctx = context_from_deps(Deps())
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    class _Scope:
        async def scope_document(self, request):
            raise AssertionError("scope port must not be consulted without identity")

    with patch.object(ctx.authz, "scope", return_value=_Scope()):
        with ctx.inv_ctx.bind(metadata=metadata):
            wrap = AuthzDocumentScopeWrap(
                spec=AuthzSpec(name="z"),
                document_name="doc",
                operation="list",
            )(ctx)

            async def _next(args: Any) -> Any:
                return args

            with pytest.raises(CoreException) as exc_info:
                await wrap(_next, None)
            assert exc_info.value.kind == ExceptionKind.AUTHENTICATION
            assert exc_info.value.code == "auth_required"


@pytest.mark.asyncio
async def test_before_authorize_allows_delegated_when_both_permitted() -> None:
    ctx = context_from_deps(Deps())
    agent = AuthnIdentity(principal_id=uuid4())
    user = AuthnIdentity(principal_id=uuid4(), actor=agent)
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with patch.object(ctx.authz, "decision", return_value=_AllowDecision()):
        with ctx.inv_ctx.bind(metadata=metadata, authn=user):
            hook = AuthzBeforeAuthorize(spec=AuthzSpec(name="z"), action="x.read")(ctx)
            await hook(None)  # subject allowed AND actor allowed


@pytest.mark.asyncio
async def test_before_authorize_denies_delegated_when_actor_not_permitted() -> None:
    ctx = context_from_deps(Deps())
    agent = AuthnIdentity(principal_id=uuid4())
    user = AuthnIdentity(principal_id=uuid4(), actor=agent)
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    # Subject (user) is allowed, but the agent is not — least-privilege intersection denies.
    decision = _AllowExceptPrincipal(agent.principal_id)

    with patch.object(ctx.authz, "decision", return_value=decision):
        with ctx.inv_ctx.bind(metadata=metadata, authn=user):
            hook = AuthzBeforeAuthorize(spec=AuthzSpec(name="z"), action="x.read")(ctx)

            with pytest.raises(CoreException) as exc_info:
                await hook(None)
            assert exc_info.value.kind == ExceptionKind.AUTHORIZATION
            assert exc_info.value.code == "delegate_denied"


@pytest.mark.asyncio
async def test_before_authorize_denies_multi_hop_when_inner_actor_not_permitted() -> None:
    ctx = context_from_deps(Deps())
    system = AuthnIdentity(principal_id=uuid4())
    agent = AuthnIdentity(principal_id=uuid4(), actor=system)
    user = AuthnIdentity(principal_id=uuid4(), actor=agent)
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    # user + agent permitted, but the innermost actor (system) is not — the chain walk denies.
    decision = _AllowExceptPrincipal(system.principal_id)

    with patch.object(ctx.authz, "decision", return_value=decision):
        with ctx.inv_ctx.bind(metadata=metadata, authn=user):
            hook = AuthzBeforeAuthorize(spec=AuthzSpec(name="z"), action="x.read")(ctx)

            with pytest.raises(CoreException) as exc_info:
                await hook(None)
            assert exc_info.value.code == "delegate_denied"


@pytest.mark.asyncio
async def test_delegation_grant_enforced_allows_when_granted() -> None:
    ctx = context_from_deps(Deps())
    agent = AuthnIdentity(principal_id=uuid4())
    user = AuthnIdentity(principal_id=uuid4(), actor=agent)
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())
    spec = AuthzSpec(name="z", enforce_delegation_grant=True)

    with (
        patch.object(ctx.authz, "decision", return_value=_AllowDecision()),
        patch.object(ctx.authz, "delegation", return_value=_AllowDelegation()),
    ):
        with ctx.inv_ctx.bind(metadata=metadata, authn=user):
            hook = AuthzBeforeAuthorize(spec=spec, action="x.read")(ctx)
            await hook(None)  # intersection OK and may_act granted


@pytest.mark.asyncio
async def test_delegation_grant_enforced_denies_when_not_granted() -> None:
    ctx = context_from_deps(Deps())
    agent = AuthnIdentity(principal_id=uuid4())
    user = AuthnIdentity(principal_id=uuid4(), actor=agent)
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())
    spec = AuthzSpec(name="z", enforce_delegation_grant=True)

    # Both independently permitted (intersection passes), but no may_act grant.
    with (
        patch.object(ctx.authz, "decision", return_value=_AllowDecision()),
        patch.object(ctx.authz, "delegation", return_value=_DenyDelegation()),
    ):
        with ctx.inv_ctx.bind(metadata=metadata, authn=user):
            hook = AuthzBeforeAuthorize(spec=spec, action="x.read")(ctx)

            with pytest.raises(CoreException) as exc_info:
                await hook(None)
            assert exc_info.value.kind == ExceptionKind.AUTHORIZATION
            assert exc_info.value.code == "delegation_not_granted"


@pytest.mark.asyncio
async def test_delegation_enforcement_fails_loud_when_port_unwired() -> None:
    # enforce_delegation_grant=True but no DelegationPort wired → fail at hook build,
    # never silently skip the may_act check.
    ctx = context_from_deps(Deps())
    spec = AuthzSpec(name="z", enforce_delegation_grant=True)

    with patch.object(ctx.authz, "decision", return_value=_AllowDecision()):
        with pytest.raises(CoreException):
            AuthzBeforeAuthorize(spec=spec, action="x.read")(ctx)


@pytest.mark.asyncio
async def test_delegation_not_consulted_when_not_enforced() -> None:
    # Default spec (enforce_delegation_grant=False): a denying delegation port is irrelevant.
    ctx = context_from_deps(Deps())
    agent = AuthnIdentity(principal_id=uuid4())
    user = AuthnIdentity(principal_id=uuid4(), actor=agent)
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with (
        patch.object(ctx.authz, "decision", return_value=_AllowDecision()),
        patch.object(ctx.authz, "delegation", return_value=_DenyDelegation()),
    ):
        with ctx.inv_ctx.bind(metadata=metadata, authn=user):
            hook = AuthzBeforeAuthorize(spec=AuthzSpec(name="z"), action="x.read")(ctx)
            await hook(None)  # passes despite denying delegation port (not consulted)


# ....................... #
# explain_empty: abstention reasons on empty pages from the scope wrap


class _ListArgs(BaseDTO):
    filters: object = None
    page: int = 1
    size: int = 10


class _NoSizeArgs(BaseDTO):
    filters: object = None


@attrs.define(kw_only=True)
class _AttrsListArgs:
    filters: object = None
    page: int = 1
    size: int = 10


class _FixedScope:
    def __init__(self, scope: AuthzDocumentScope) -> None:
        self.scope = scope

    async def scope_document(self, request: Any) -> AuthzDocumentScope:
        _ = request
        return self.scope


def _empty_page() -> CountlessPage[str]:
    return CountlessPage(hits=[], page=1, size=10)


_POLICY = AuthzDocumentScope(filters={"$values": {"owner": "me"}})
_UNRESTRICTED = AuthzDocumentScope()


class TestExplainEmpty:
    def _bound_ctx(self, scope: AuthzDocumentScope) -> tuple[Any, Any, Any]:
        ctx = context_from_deps(Deps())
        ident = AuthnIdentity(principal_id=uuid4())
        metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

        return (
            ctx,
            patch.object(ctx.authz, "scope", return_value=_FixedScope(scope)),
            ctx.inv_ctx.bind(metadata=metadata, authn=ident),
        )

    def _wrap(self, ctx: Any, *, explain_empty: bool = True) -> Any:
        return AuthzDocumentScopeWrap(
            spec=AuthzSpec(name="z"),
            document_name="doc",
            operation="list",
            explain_empty=explain_empty,
        )(ctx)

    # ....................... #

    @pytest.mark.asyncio
    async def test_flag_off_leaves_empty_page_alone(self) -> None:
        ctx, patcher, binder = self._bound_ctx(_POLICY)
        calls: list[object] = []

        async def _next(args: Any) -> Any:
            calls.append(args)
            return _empty_page()

        with patcher, binder:
            result = await self._wrap(ctx, explain_empty=False)(_next, _ListArgs())

        assert result.abstention is None
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_unrestricted_empty_page_is_no_match_without_probe(self) -> None:
        ctx, patcher, binder = self._bound_ctx(_UNRESTRICTED)
        calls: list[object] = []

        async def _next(args: Any) -> Any:
            calls.append(args)
            return _empty_page()

        with patcher, binder:
            result = await self._wrap(ctx)(_next, _ListArgs())

        assert result.abstention == "no_match"
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_policy_filtered_empty_page_probes_to_not_permitted(self) -> None:
        base = {"$values": {"status": "open"}}
        ctx, patcher, binder = self._bound_ctx(_POLICY)
        calls: list[_ListArgs] = []

        async def _next(args: Any) -> Any:
            calls.append(args)

            if len(calls) == 1:
                return _empty_page()

            return CountlessPage(hits=["hidden"], page=1, size=1)

        with patcher, binder:
            token = ctx.inv_ctx.set_read_only()

            try:
                result = await self._wrap(ctx)(
                    _next, _ListArgs(filters=base, page=3, size=50)
                )
            finally:
                ctx.inv_ctx.reset_read_only(token)

        assert result.abstention == "not_permitted"
        assert result.hits == []
        assert len(calls) == 2

        # The scoped read got the merged filter; the probe got the caller's own filter,
        # clamped to a single first-page row.
        scoped, probe = calls
        assert scoped.filters == {"$and": [base, _POLICY.filters]}
        assert probe.filters == base
        assert (probe.page, probe.size) == (1, 1)

    @pytest.mark.asyncio
    async def test_policy_filtered_empty_page_probes_to_no_match(self) -> None:
        ctx, patcher, binder = self._bound_ctx(_POLICY)

        async def _next(args: Any) -> Any:
            _ = args
            return _empty_page()

        with patcher, binder:
            token = ctx.inv_ctx.set_read_only()

            try:
                result = await self._wrap(ctx)(_next, _ListArgs())
            finally:
                ctx.inv_ctx.reset_read_only(token)

        assert result.abstention == "no_match"

    @pytest.mark.asyncio
    async def test_non_empty_page_is_untouched(self) -> None:
        ctx, patcher, binder = self._bound_ctx(_POLICY)
        calls: list[object] = []

        async def _next(args: Any) -> Any:
            calls.append(args)
            return CountlessPage(hits=["a"], page=1, size=10)

        with patcher, binder:
            result = await self._wrap(ctx)(_next, _ListArgs())

        assert result.abstention is None
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_unclampable_args_skip_the_probe(self) -> None:
        # No ``size`` field to clamp: an unbounded ungated probe is exactly the query
        # the policy filter exists to prevent, so the page stays unexplained.
        ctx, patcher, binder = self._bound_ctx(_POLICY)
        calls: list[object] = []

        async def _next(args: Any) -> Any:
            calls.append(args)
            return _empty_page()

        with patcher, binder:
            token = ctx.inv_ctx.set_read_only()

            try:
                result = await self._wrap(ctx)(_next, _NoSizeArgs())
            finally:
                ctx.inv_ctx.reset_read_only(token)

        assert result.abstention is None
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_probe_failure_returns_the_real_result(self) -> None:
        ctx, patcher, binder = self._bound_ctx(_POLICY)
        calls: list[object] = []

        async def _next(args: Any) -> Any:
            calls.append(args)

            if len(calls) == 1:
                return _empty_page()

            raise RuntimeError("backend blip")

        with patcher, binder:
            token = ctx.inv_ctx.set_read_only()

            try:
                result = await self._wrap(ctx)(_next, _ListArgs())
            finally:
                ctx.inv_ctx.reset_read_only(token)

        assert result.abstention is None
        assert result.hits == []

    @pytest.mark.asyncio
    async def test_pydantic_page_result_is_stamped_via_model_copy(self) -> None:
        class _PageDTO(BaseDTO):
            hits: list[str]
            abstention: str | None = None

        ctx, patcher, binder = self._bound_ctx(_UNRESTRICTED)

        async def _next(args: Any) -> Any:
            _ = args
            return _PageDTO(hits=[])

        with patcher, binder:
            result = await self._wrap(ctx)(_next, _ListArgs())

        assert isinstance(result, _PageDTO)
        assert result.abstention == "no_match"

    @pytest.mark.asyncio
    async def test_probe_skipped_when_invocation_is_not_read_only(self) -> None:
        # A probe re-invokes the inner chain, and only a QUERY invocation is known
        # read-only — a COMMAND-classified operation must not replay its handler.
        ctx, patcher, binder = self._bound_ctx(_POLICY)
        calls: list[Any] = []

        async def _next(args: Any) -> Any:
            calls.append(args)
            return _empty_page()

        with patcher, binder:
            result = await self._wrap(ctx)(_next, _ListArgs())

        assert result.abstention is None
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_attrs_args_probe_to_not_permitted(self) -> None:
        base = {"$values": {"status": "open"}}
        ctx, patcher, binder = self._bound_ctx(_POLICY)
        calls: list[Any] = []

        async def _next(args: Any) -> Any:
            calls.append(args)

            if len(calls) == 1:
                return _empty_page()

            return CountlessPage(hits=["hidden"], page=1, size=1)

        with patcher, binder:
            token = ctx.inv_ctx.set_read_only()

            try:
                result = await self._wrap(ctx)(
                    _next, _AttrsListArgs(filters=base, page=3, size=50)
                )
            finally:
                ctx.inv_ctx.reset_read_only(token)

        assert result.abstention == "not_permitted"
        assert len(calls) == 2
        probe = calls[1]
        assert probe.filters == base
        assert (probe.page, probe.size) == (1, 1)

    def test_probe_args_refused_for_unknown_args_shape(self) -> None:
        from forze.application.hooks.authz.plans import _existence_probe_args

        class _Plain:
            filters = None
            size = 10

        assert _existence_probe_args(_Plain(), None, "filters") is None

    @pytest.mark.asyncio
    async def test_non_page_result_is_untouched(self) -> None:
        ctx, patcher, binder = self._bound_ctx(_POLICY)

        async def _next(args: Any) -> Any:
            _ = args
            return None

        with patcher, binder:
            result = await self._wrap(ctx)(_next, _ListArgs())

        assert result is None
