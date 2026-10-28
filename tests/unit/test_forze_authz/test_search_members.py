"""``AuthzSearchMembers``: each hub or federated search member passes its own read action."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch
from uuid import uuid4

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import (
    AuthzDecision,
    AuthzDocumentScope,
    AuthzSpec,
)
from forze.application.contracts.idempotency import IdempotencySpec
from forze.application.contracts.search import (
    FederatedSearchQueryDepKey,
    FederatedSearchSpec,
    HubSearchQueryDepKey,
    HubSearchSpec,
    MultiSourceSearchOptions,
    SearchMemberGrant,
    SearchSpec,
    bind_search_member_grant,
    search_member_grant,
)
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from forze.application.hooks.authn import AuthnRequired
from forze.application.execution.operations import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.hooks.authz import (
    AuthzBeforeAuthorize,
    AuthzDocumentScopeWrap,
    AuthzSearchMembers,
)
from forze.application.hooks.idempotency import IdempotencyWrap
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_kits.aggregates.search import (
    SearchFacade,
    SearchKernelOp,
    SearchMappers,
    SearchPaginated,
    SearchRequestDTO,
    build_federated_search_registry,
    build_hub_search_registry,
)
from forze_mock.adapters.search.command import MockSearchCommandAdapter
from forze_mock.adapters.search.federated import MockFederatedSearchAdapter
from forze_mock.adapters.search.hub import MockHubSearchAdapter
from forze_mock.adapters.search.query import MockSearchAdapter
from forze_mock.state import MockState
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.unit

# ----------------------- #


class _Order(BaseModel):
    id: str
    title: str


class _Invoice(BaseModel):
    id: str
    title: str
    owner: str


ORDERS = SearchSpec(name="orders", model_type=_Order, fields=["title"])
INVOICES = SearchSpec(name="invoices", model_type=_Invoice, fields=["title"])
FED = FederatedSearchSpec(name="everything", members=[ORDERS, INVOICES])

NOTES = SearchSpec(name="order_notes", model_type=_Order, fields=["title"])
HUB = HubSearchSpec(name="order_hub", model_type=_Order, members=[ORDERS, NOTES])

AUTHZ = AuthzSpec(name="api")
FED_ACTIONS = {"orders": "orders:read", "invoices": "invoices:read"}
HUB_ACTIONS = {"orders": "orders:read", "order_notes": "notes:read"}


@attrs.define
class _Role:
    """A decision port granting exactly the listed actions."""

    allowed: frozenset[str]
    asked: list[str] = attrs.field(factory=list)

    async def authorize(self, request: Any) -> AuthzDecision:
        self.asked.append(request.action)

        if request.action in self.allowed:
            return AuthzDecision(allowed=True, matched_permission_key=request.action)

        return AuthzDecision(allowed=False, reason=f"Permission denied: {request.action!r}")


@attrs.define
class _Scope:
    """A scope port with fixed row filters per document; ``None`` denies the document."""

    filters: dict[str, Any]
    requests: list[Any] = attrs.field(factory=list)

    async def scope_document(self, request: Any) -> AuthzDocumentScope:
        self.requests.append(request)

        if request.document_name in self.filters and self.filters[request.document_name] is None:
            return AuthzDocumentScope(deny_all=True, reason="scope denied")

        return AuthzDocumentScope(filters=self.filters.get(request.document_name))


async def _state() -> MockState:
    state = MockState()
    await MockSearchCommandAdapter(state=state, spec=ORDERS).upsert(
        [_Order(id="o1", title="alpha order")]
    )
    await MockSearchCommandAdapter(state=state, spec=NOTES).upsert(
        [_Order(id="n1", title="alpha note")]
    )
    await MockSearchCommandAdapter(state=state, spec=INVOICES).upsert(
        [
            _Invoice(id="i1", title="alpha invoice", owner="u1"),
            _Invoice(id="i2", title="alpha invoice", owner="u2"),
        ]
    )
    return state


def _ctx(state: MockState) -> ExecutionContext:
    def _federated(_ctx: Any, spec: FederatedSearchSpec[Any]) -> Any:
        legs = [(m.name, MockSearchAdapter(state=state, spec=m)) for m in spec.members]
        return MockFederatedSearchAdapter(federated_spec=spec, legs=legs)

    def _hub(_ctx: Any, spec: HubSearchSpec[Any]) -> Any:
        legs = [(m.name, MockSearchAdapter(state=state, spec=m)) for m in spec.members]
        return MockHubSearchAdapter(hub_spec=spec, legs=legs)

    return context_from_deps(
        Deps.routed(
            {
                FederatedSearchQueryDepKey: {FED.name: _federated},
                HubSearchQueryDepKey: {HUB.name: _hub},
            }
        )
    )


def _guarded(registry: Any, spec: Any, *steps: Any) -> Any:
    return (
        registry.bind(spec.default_namespace.key(SearchKernelOp.TYPED))
        .bind_outer()
        .before(AuthnRequired().to_step())
        .wrap(*steps)
        .finish(deep=True)
        .freeze()
    )


def _fed_registry(*, mapper_options: Any = ..., **hook: Any) -> Any:
    members = AuthzSearchMembers(spec=AUTHZ, search=FED, actions=FED_ACTIONS, **hook)
    mappers: SearchMappers[Any] = SearchMappers()

    if mapper_options is not ...:
        # App code inside the handler, after the hook: it rewrites the request's options.
        async def _map(dto: Any) -> Any:
            return dto.model_copy(update={"options": mapper_options})

        mappers = SearchMappers(search=lambda _ctx: _map)

    return _guarded(build_federated_search_registry(FED, mappers), FED, members.to_step())


async def _search(
    ctx: ExecutionContext,
    registry: Any,
    spec: Any,
    *,
    role: _Role,
    scope: _Scope | None = None,
    **options: Any,
) -> set[tuple[str, str]]:
    facade = SearchFacade(ctx=ctx, registry=registry, namespace=spec.default_namespace)
    dto = SearchRequestDTO[MultiSourceSearchOptions](query="alpha", options=options or None)
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with (
        patch.object(ctx.authz, "decision", return_value=role),
        patch.object(ctx.authz, "scope", return_value=scope),
        ctx.inv_ctx.bind(metadata=metadata, authn=AuthnIdentity(principal_id=uuid4())),
    ):
        page = await facade.search(dto)

    if isinstance(spec, HubSearchSpec):
        return {("hub", hit.id) for hit in page.hits}

    return {(hit.member, hit.hit.id) for hit in page.hits}


ORDERS_ONLY = _Role(allowed=frozenset({"orders:read"}))
BOTH = _Role(allowed=frozenset({"orders:read", "invoices:read"}))

# ....................... #


@pytest.mark.asyncio
async def test_a_role_reading_orders_only_sees_no_invoice_hits() -> None:
    ctx = _ctx(await _state())

    hits = await _search(ctx, _fed_registry(), FED, role=ORDERS_ONLY)

    assert hits == {("orders", "o1")}


@pytest.mark.asyncio
async def test_a_role_reading_both_members_sees_both() -> None:
    ctx = _ctx(await _state())

    hits = await _search(ctx, _fed_registry(), FED, role=BOTH)

    assert hits == {("orders", "o1"), ("invoices", "i1"), ("invoices", "i2")}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "options",
    [
        {"members": ["invoices"]},
        {"members": ["orders", "invoices"]},
        {"member_weights": {"invoices": 0.5}},
    ],
)
async def test_a_request_naming_a_member_the_caller_may_not_read_is_refused(
    options: dict[str, Any],
) -> None:
    ctx = _ctx(await _state())

    with pytest.raises(CoreException) as exc_info:
        await _search(ctx, _fed_registry(), FED, role=ORDERS_ONLY, **options)

    assert exc_info.value.kind is ExceptionKind.AUTHORIZATION
    assert exc_info.value.code == "permission_denied"
    assert "invoices:read" in exc_info.value.summary


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "options",
    [
        {"members": ["orders"]},
        {"member_weights": {"orders": 1.0, "invoices": 0.0}},
        # Unnamed but still weighted: the request names no forbidden member, so it narrows.
        {"member_weights": {"orders": 0.5}},
    ],
)
async def test_a_request_naming_only_readable_members_runs(options: dict[str, Any]) -> None:
    ctx = _ctx(await _state())

    hits = await _search(ctx, _fed_registry(), FED, role=ORDERS_ONLY, **options)

    assert hits == {("orders", "o1")}


@pytest.mark.asyncio
async def test_a_role_reading_no_member_is_refused() -> None:
    ctx = _ctx(await _state())

    with pytest.raises(CoreException) as exc_info:
        await _search(ctx, _fed_registry(), FED, role=_Role(allowed=frozenset()))

    assert exc_info.value.code == "permission_denied"


@pytest.mark.asyncio
async def test_member_scope_filters_apply_to_their_own_leg_alone() -> None:
    ctx = _ctx(await _state())
    # Orders have no ``owner`` field, so the invoices filter on the orders leg would drop o1.
    scope = _Scope(filters={"invoices": {"$values": {"owner": "u1"}}})

    hits = await _search(ctx, _fed_registry(scope_operation="search"), FED, role=BOTH, scope=scope)

    assert hits == {("orders", "o1"), ("invoices", "i1")}
    assert {(r.document_name, r.operation, r.action) for r in scope.requests} == {
        ("orders", "search", "orders:read"),
        ("invoices", "search", "invoices:read"),
    }


@pytest.mark.asyncio
async def test_member_scope_filters_conjoin_with_the_callers_own() -> None:
    ctx = _ctx(await _state())
    scope = _Scope(filters={"invoices": {"$values": {"owner": "u1"}}})

    hits = await _search(
        ctx,
        _fed_registry(scope_operation="search"),
        FED,
        role=BOTH,
        scope=scope,
        member_filters={"invoices": {"$values": {"id": "i2"}}},
    )

    assert hits == {("orders", "o1")}


@pytest.mark.asyncio
async def test_a_member_the_scope_denies_is_dropped_from_an_all_members_request() -> None:
    ctx = _ctx(await _state())
    scope = _Scope(filters={"invoices": None})

    hits = await _search(ctx, _fed_registry(scope_operation="search"), FED, role=BOTH, scope=scope)

    assert hits == {("orders", "o1")}

    with pytest.raises(CoreException) as exc_info:
        await _search(
            ctx,
            _fed_registry(scope_operation="search"),
            FED,
            role=BOTH,
            scope=scope,
            members=["invoices"],
        )

    assert exc_info.value.code == "scope_denied"


@pytest.mark.asyncio
async def test_a_hub_member_the_caller_may_not_read_is_dropped_or_refused() -> None:
    ctx = _ctx(await _state())
    members = AuthzSearchMembers(spec=AUTHZ, search=HUB, actions=HUB_ACTIONS)
    registry = _guarded(build_hub_search_registry(HUB), HUB, members.to_step())

    assert await _search(ctx, registry, HUB, role=ORDERS_ONLY) == {("hub", "o1")}

    with pytest.raises(CoreException) as exc_info:
        await _search(ctx, registry, HUB, role=ORDERS_ONLY, members=["order_notes"])

    assert exc_info.value.code == "permission_denied"

    # A fresh context: the resolved hook keeps the decision port it was built with.
    both = _Role(allowed=frozenset({"orders:read", "notes:read"}))
    hits = await _search(_ctx(await _state()), registry, HUB, role=both)
    assert hits == {("hub", "o1"), ("hub", "n1")}


# ....................... #


@pytest.mark.parametrize(
    ("spec", "build"),
    [(FED, build_federated_search_registry), (HUB, build_hub_search_registry)],
)
def test_an_operation_level_permission_alone_refuses_to_freeze(spec: Any, build: Any) -> None:
    surface = AuthzBeforeAuthorize(spec=AUTHZ, action="search:read").to_step()

    with pytest.raises(CoreException) as exc_info:
        (
            build(spec)
            .bind(spec.default_namespace.key(SearchKernelOp.TYPED))
            .bind_outer()
            .before(AuthnRequired().to_step(), surface)
            .finish(deep=True)
            .freeze()
        )

    assert exc_info.value.kind is ExceptionKind.CONFIGURATION
    assert exc_info.value.code == "member_authz_missing"


def test_a_shared_document_scope_alone_refuses_to_freeze() -> None:
    scope = AuthzDocumentScopeWrap(spec=AUTHZ, document_name="everything", operation="search")

    with pytest.raises(CoreException, match="member_authz_missing|authorizes no read"):
        _guarded(build_federated_search_registry(FED), FED, scope.to_step(step_id="authz.scope"))


def test_an_unguarded_search_and_a_member_guarded_one_freeze() -> None:
    build_federated_search_registry(FED).freeze()
    catalog = _fed_registry().catalog()
    entry = catalog[FED.default_namespace.key(SearchKernelOp.TYPED)]

    assert entry.required_permissions == ("invoices:read", "orders:read")


def test_the_surface_permission_can_still_read_every_member_when_mapped_explicitly() -> None:
    surface = AuthzBeforeAuthorize(spec=AUTHZ, action="search:read").to_step()
    members = AuthzSearchMembers(
        spec=AUTHZ, search=FED, actions=dict.fromkeys(FED_ACTIONS, "search:read")
    )

    (
        build_federated_search_registry(FED)
        .bind(FED.default_namespace.key(SearchKernelOp.TYPED))
        .bind_outer()
        .before(AuthnRequired().to_step(), surface)
        .wrap(members.to_step())
        .finish(deep=True)
        .freeze()
    )


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"search": FED, "actions": {"orders": "orders:read"}}, "no read action"),
        ({"search": FED, "actions": {**FED_ACTIONS, "notes": "x"}}, "no member for"),
        ({"search": HUB, "actions": HUB_ACTIONS, "scope_operation": "search"}, "hub"),
        (
            {"search": FED, "actions": FED_ACTIONS, "scope_documents": {"orders": "o"}},
            "no scope_operation",
        ),
        (
            {
                "search": FED,
                "actions": FED_ACTIONS,
                "scope_operation": "search",
                "scope_documents": {"notes": "n"},
            },
            "no member for",
        ),
    ],
)
def test_a_misconfigured_hook_is_refused(kwargs: dict[str, Any], match: str) -> None:
    with pytest.raises(CoreException, match=match):
        AuthzSearchMembers(spec=AUTHZ, **kwargs)


# ....................... #
# A request mapper runs inside the handler, after the hook: the adapter holds the line.


@pytest.mark.asyncio
async def test_a_mapper_naming_a_forbidden_member_is_refused() -> None:
    ctx = _ctx(await _state())
    registry = _fed_registry(mapper_options={"members": ["orders", "invoices"]})

    with pytest.raises(CoreException) as exc_info:
        await _search(ctx, registry, FED, role=ORDERS_ONLY)

    assert exc_info.value.code == "permission_denied"
    assert "invoices:read" in exc_info.value.summary


@pytest.mark.asyncio
async def test_a_mapper_widening_to_every_member_skips_the_forbidden_one() -> None:
    ctx = _ctx(await _state())
    registry = _fed_registry(mapper_options=None)

    hits = await _search(ctx, registry, FED, role=ORDERS_ONLY, members=["orders"])

    assert hits == {("orders", "o1")}


@pytest.mark.asyncio
async def test_a_mapper_naming_a_readable_member_reaches_it() -> None:
    ctx = _ctx(await _state())
    registry = _fed_registry(mapper_options={"members": ["orders", "invoices"]})

    hits = await _search(ctx, registry, FED, role=BOTH, members=["orders"])

    assert hits == {("orders", "o1"), ("invoices", "i1"), ("invoices", "i2")}


@pytest.mark.asyncio
async def test_a_mapper_dropping_member_filters_keeps_the_policy_filter() -> None:
    ctx = _ctx(await _state())
    scope = _Scope(filters={"invoices": {"$values": {"owner": "u1"}}})
    registry = _fed_registry(mapper_options=None, scope_operation="search")

    hits = await _search(
        ctx,
        registry,
        FED,
        role=BOTH,
        scope=scope,
        member_filters={"invoices": {"$values": {"id": "i1"}}},
    )

    assert hits == {("orders", "o1"), ("invoices", "i1")}


@pytest.mark.asyncio
async def test_a_hub_mapper_naming_a_forbidden_member_is_refused() -> None:
    ctx = _ctx(await _state())

    async def _map(dto: Any) -> Any:
        return dto.model_copy(update={"options": {"members": ["order_notes"]}})

    members = AuthzSearchMembers(spec=AUTHZ, search=HUB, actions=HUB_ACTIONS)
    registry = _guarded(
        build_hub_search_registry(HUB, SearchMappers(search=lambda _ctx: _map)),
        HUB,
        members.to_step(),
    )

    with pytest.raises(CoreException) as exc_info:
        await _search(ctx, registry, HUB, role=ORDERS_ONLY)

    assert exc_info.value.code == "permission_denied"


@pytest.mark.asyncio
async def test_scope_documents_names_a_members_policy_document() -> None:
    ctx = _ctx(await _state())
    scope = _Scope(filters={"invoice_docs": {"$values": {"owner": "u2"}}})
    registry = _fed_registry(scope_operation="search", scope_documents={"invoices": "invoice_docs"})

    hits = await _search(ctx, registry, FED, role=BOTH, scope=scope)

    assert hits == {("orders", "o1"), ("invoices", "i2")}
    assert {r.document_name for r in scope.requests} == {"orders", "invoice_docs"}


# ....................... #
# Stacked and nested grants, decisions per action, and what the freeze gate refuses.


@pytest.mark.asyncio
@pytest.mark.parametrize("strict_priority", [40, 50])
async def test_stacked_member_hooks_allow_only_what_both_allow(strict_priority: int) -> None:
    ctx = _ctx(await _state())
    strict = AuthzSearchMembers(spec=AUTHZ, search=FED, actions=FED_ACTIONS)
    broad = AuthzSearchMembers(
        spec=AUTHZ, search=FED, actions=dict.fromkeys(FED_ACTIONS, "search:read")
    )
    registry = _guarded(
        build_federated_search_registry(FED),
        FED,
        strict.to_step(step_id="authz.strict", priority=strict_priority),
        broad.to_step(step_id="authz.broad", priority=90 - strict_priority),
    )
    role = _Role(allowed=frozenset({"orders:read", "search:read"}))

    assert await _search(ctx, registry, FED, role=role) == {("orders", "o1")}

    with pytest.raises(CoreException) as exc_info:
        await _search(ctx, registry, FED, role=role, members=["invoices"])

    assert exc_info.value.code == "permission_denied"


def test_a_nested_grant_intersects_with_the_bound_one() -> None:
    denial = exc.authorization("no invoices", code="permission_denied")
    outer = SearchMemberGrant(
        allowed=frozenset({"orders"}),
        denials={"invoices": denial},
        filters={"orders": {"$values": {"a": 1}}},
    )
    inner = SearchMemberGrant(
        allowed=frozenset({"orders", "invoices"}),
        filters={"orders": {"$values": {"b": 2}}},
    )

    with bind_search_member_grant(FED.name, outer), bind_search_member_grant(FED.name, inner):
        grant = search_member_grant(FED.name)

        assert grant is not None
        assert grant.allowed == {"orders"}
        assert grant.denials["invoices"] is denial
        assert grant.filters == {"orders": {"$and": [{"$values": {"a": 1}}, {"$values": {"b": 2}}]}}

    assert search_member_grant(FED.name) is None


@pytest.mark.asyncio
async def test_members_sharing_an_action_take_one_decision() -> None:
    ctx = _ctx(await _state())
    members = AuthzSearchMembers(
        spec=AUTHZ, search=FED, actions=dict.fromkeys(FED_ACTIONS, "search:read")
    )
    registry = _guarded(build_federated_search_registry(FED), FED, members.to_step())
    role = _Role(allowed=frozenset({"search:read"}))

    await _search(ctx, registry, FED, role=role)

    assert role.asked == ["search:read"]


def test_a_descriptor_override_dropping_members_is_refused() -> None:
    key = FED.default_namespace.key(SearchKernelOp.TYPED)
    registry = build_federated_search_registry(FED)
    bare = OperationDescriptor(description="search")

    with pytest.raises(CoreException) as exc_info:
        registry.set_descriptor(key, bare, override=True)

    assert exc_info.value.code == "descriptor_members_dropped"

    with pytest.raises(CoreException, match="drops member"):
        registry.set_descriptors({key: bare}, override=True)

    with pytest.raises(CoreException, match="drops member"):
        OperationRegistry.merge(
            registry, OperationRegistry().set_descriptor(key, bare), override=True
        )

    kept = attrs.evolve(registry.get_descriptors()[key], description="search")
    registry.set_descriptor(key, kept, override=True)


def test_a_replaying_wrap_on_a_member_guarded_search_refuses_to_freeze() -> None:
    replay = IdempotencyWrap(
        op="search", spec=IdempotencySpec(name="search"), result_type=SearchPaginated
    )
    members = AuthzSearchMembers(spec=AUTHZ, search=FED, actions=FED_ACTIONS)

    with pytest.raises(CoreException) as exc_info:
        _guarded(build_federated_search_registry(FED), FED, replay.to_step(), members.to_step())

    assert exc_info.value.code == "member_authz_replay"
