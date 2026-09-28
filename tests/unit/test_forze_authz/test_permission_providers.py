"""Derived permissions join the decision, and a derived denial outranks every other source.

The denial legs run through the resolver rather than a hand-built ``EffectiveGrants``: a direct
binding, a role and a group each take a different path through it, and a rule that holds for one
path and not another is exactly what a hand-built snapshot would never show.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.contracts.authz import (
    AuthzRequest,
    AuthzResource,
    AuthzSubject,
    DerivedPermissions,
    EffectiveGrants,
    PermissionProvider,
)
from forze.base.exceptions import CoreException
from forze.testing import context_from_modules
from forze_identity.authz import (
    AuthzKernelConfig,
    build_authz_shared_services,
    permission_providers_lifecycle_step,
)
from forze_identity.authz.application.specs import permission_definition_spec
from forze_identity.authz.domain.models.bindings import (
    ReadGroupPermissionBinding,
    ReadGroupPrincipalBinding,
    ReadPrincipalPermissionBinding,
    ReadPrincipalRoleBinding,
    ReadRolePermissionBinding,
)
from forze_identity.authz.domain.models.permission_definition import (
    CreatePermissionDefinitionCmd,
)
from forze_identity.authz.services.grants import AuthzGrantResolver
from forze_identity.authz.services.policy import AuthzPolicyService
from forze_mock import MockDepsModule
from tests.unit.test_forze_authz.test_grant_resolver_service import (
    FakeDocQuery,
    _doc_kwargs,
    _empty_deps,
    _group,
    _perm,
    _role,
)

# ----------------------- #

POLICY = AuthzPolicyService()
PRINCIPAL = uuid4()


@attrs.define(slots=True, kw_only=True)
class _Provider:
    name: str
    keys: frozenset[str]
    granted: frozenset[str] = frozenset()
    denied: frozenset[str] = frozenset()
    fails: bool = False
    seen: list[UUID] = attrs.field(factory=list)

    async def derive(self, principal_id: UUID, ctx: Any) -> DerivedPermissions:
        self.seen.append(principal_id)

        if self.fails:
            raise RuntimeError("members table unreachable")

        return DerivedPermissions(granted=self.granted, denied=self.denied)


@attrs.define(slots=True, kw_only=True)
class _Returns:
    """A provider whose result is whatever *make* builds, well-formed or not."""

    name: str
    keys: frozenset[str]
    make: Any

    async def derive(self, principal_id: UUID, ctx: Any) -> DerivedPermissions:
        return self.make()


def _request(action: str, *, owner: UUID | None = None) -> AuthzRequest:
    resource = (
        AuthzResource(resource_type="invoice", attributes={"owner_id": owner})
        if owner is not None
        else None
    )

    return AuthzRequest(
        subject=AuthzSubject(principal_id=PRINCIPAL), action=action, resource=resource
    )


async def _grants(deps: Any = None, *providers: PermissionProvider) -> EffectiveGrants:
    resolver = AuthzGrantResolver(
        deps=deps if deps is not None else _empty_deps(),
        providers=providers,
        ctx=context_from_modules(MockDepsModule()),
    )

    return await resolver.resolve_effective_grants(PRINCIPAL)


async def _allowed(action: str, deps: Any = None, *providers: PermissionProvider) -> bool:
    return POLICY.decide(await _grants(deps, *providers), _request(action)).allowed


def _direct_binding(key: str) -> Any:
    perm = _perm(uuid4(), key)

    return _empty_deps(
        permission_qry=FakeDocQuery(by_id={perm.id: perm}),
        pp_binding_qry=FakeDocQuery(
            rows=[
                ReadPrincipalPermissionBinding(
                    **_doc_kwargs(uuid4()), principal_id=PRINCIPAL, permission_id=perm.id
                )
            ]
        ),
    )


def _role_binding(key: str) -> Any:
    perm, role = _perm(uuid4(), key), _role(uuid4(), "clerk")

    return _empty_deps(
        permission_qry=FakeDocQuery(by_id={perm.id: perm}),
        role_qry=FakeDocQuery(by_id={role.id: role}),
        pr_binding_qry=FakeDocQuery(
            rows=[
                ReadPrincipalRoleBinding(
                    **_doc_kwargs(uuid4()), principal_id=PRINCIPAL, role_id=role.id
                )
            ]
        ),
        rp_binding_qry=FakeDocQuery(
            rows=[
                ReadRolePermissionBinding(
                    **_doc_kwargs(uuid4()), role_id=role.id, permission_id=perm.id
                )
            ]
        ),
    )


def _group_binding(key: str) -> Any:
    perm, group = _perm(uuid4(), key), _group(uuid4(), "ledger", is_active=True)

    return _empty_deps(
        permission_qry=FakeDocQuery(by_id={perm.id: perm}),
        group_qry=FakeDocQuery(by_id={group.id: group}),
        gp_binding_qry=FakeDocQuery(
            rows=[
                ReadGroupPrincipalBinding(
                    **_doc_kwargs(uuid4()), group_id=group.id, principal_id=PRINCIPAL
                )
            ]
        ),
        gperm_binding_qry=FakeDocQuery(
            rows=[
                ReadGroupPermissionBinding(
                    **_doc_kwargs(uuid4()), group_id=group.id, permission_id=perm.id
                )
            ]
        ),
    )


# ....................... #


class TestTheUnion:
    async def test_a_provider_admits_without_a_binding(self) -> None:
        member = _Provider(
            name="members", keys=frozenset({"ledger.write"}), granted=frozenset({"ledger.write"})
        )

        assert await _allowed("ledger.write", None, member)

    async def test_a_binding_admits_without_a_provider(self) -> None:
        assert await _allowed("ledger.write", _direct_binding("ledger.write"))

    async def test_both_admit_once(self) -> None:
        member = _Provider(
            name="members", keys=frozenset({"ledger.write"}), granted=frozenset({"ledger.write"})
        )
        grants = await _grants(_direct_binding("ledger.write"), member)

        assert POLICY.decide(grants, _request("ledger.write")).allowed
        assert grants.granted_keys == {"ledger.write"}

    async def test_no_source_denies(self) -> None:
        assert not await _allowed("ledger.write")


class TestDeactivationWins:
    @pytest.mark.parametrize(
        "binding", [_direct_binding, _role_binding, _group_binding], ids=["direct", "role", "group"]
    )
    async def test_a_denial_masks_every_binding_path(self, binding: Any) -> None:
        inactive = _Provider(
            name="members", keys=frozenset({"ledger.write"}), denied=frozenset({"ledger.write"})
        )
        deps = binding("ledger.write")

        assert await _allowed("ledger.write", deps)  # the binding alone admits

        decision = POLICY.decide(await _grants(deps, inactive), _request("ledger.write"))

        assert not decision.allowed
        # Denied, not merely ungranted: the reason is what an operator reads to learn why.
        assert decision.reason == "Permission 'ledger.write' is denied"

    @pytest.mark.parametrize("order", ["grant-first", "deny-first"])
    async def test_order_does_not_decide(self, order: str) -> None:
        grants = _Provider(
            name="config", keys=frozenset({"ledger.write"}), granted=frozenset({"ledger.write"})
        )
        denies = _Provider(
            name="members", keys=frozenset({"ledger.write"}), denied=frozenset({"ledger.write"})
        )
        providers = (grants, denies) if order == "grant-first" else (denies, grants)

        assert not await _allowed("ledger.write", None, *providers)

    async def test_a_failing_provider_denies_what_it_declares(self) -> None:
        # An outage must not become a bypass: the binding stays masked while the read fails.
        broken = _Provider(name="members", keys=frozenset({"ledger.write"}), fails=True)

        assert not await _allowed("ledger.write", _direct_binding("ledger.write"), broken)

    async def test_an_undeclared_key_voids_the_result(self) -> None:
        # A key outside the declaration means the result cannot be read as meant: a misspelt
        # denial would deny the misspelling and leave the real key granted by the catalog. So
        # the whole result is treated like a failure.
        sloppy = _Provider(
            name="members",
            keys=frozenset({"ledger.read", "ledger.write"}),
            denied=frozenset({"ledger.wirte"}),
        )
        grants = await _grants(_direct_binding("ledger.write"), sloppy)

        assert not POLICY.decide(grants, _request("ledger.write")).allowed
        assert {(r.permission_key, r.denied) for r in grants.derived} == {
            ("ledger.read", True),
            ("ledger.write", True),
            ("ledger.wirte", True),
        }

    @pytest.mark.parametrize(
        "result",
        [
            lambda: DerivedPermissions(denied="ledger.write"),  # type: ignore[arg-type]
            lambda: DerivedPermissions(granted=["ledger.write", 7]),  # type: ignore[list-item]
            lambda: None,
            lambda: {"denied": {"ledger.write"}},
            # Shaped like the real thing, so only the type check refuses it.
            lambda: SimpleNamespace(granted=frozenset({"ledger.write"}), denied=frozenset()),
        ],
        ids=["string-denial", "non-string-key", "none", "dict", "look-alike"],
    )
    async def test_a_malformed_result_denies_what_it_declares(self, result: Any) -> None:
        odd = _Returns(name="members", keys=frozenset({"ledger.write"}), make=result)
        grants = await _grants(_direct_binding("ledger.write"), odd)

        assert not POLICY.decide(grants, _request("ledger.write")).allowed
        # Only its own keys: the principal's other permissions are decided as before.
        assert "ledger.read" not in grants.denied_keys

    async def test_an_inactive_principal_is_refused_whatever_it_holds(self) -> None:
        member = _Provider(
            name="members", keys=frozenset({"ledger.write"}), granted=frozenset({"ledger.write"})
        )
        grants = await _grants(_direct_binding("ledger.write"), member)

        decision = POLICY.decide(grants, _request("ledger.write"), principal_active=False)

        assert not decision.allowed


class TestTheSnapshot:
    async def test_a_derived_grant_is_kept_apart_and_attributed(self) -> None:
        member = _Provider(
            name="members", keys=frozenset({"ledger.write"}), granted=frozenset({"ledger.write"})
        )
        grants = await _grants(None, member)

        assert grants.permissions == frozenset()
        assert [(ref.permission_key, ref.provider, ref.denied) for ref in grants.derived] == [
            ("ledger.write", "members", False)
        ]

    async def test_providers_without_a_context_are_refused(self) -> None:
        member = _Provider(name="members", keys=frozenset({"ledger.write"}))
        resolver = AuthzGrantResolver(deps=_empty_deps(), providers=(member,))

        with pytest.raises(CoreException):
            await resolver.resolve_effective_grants(PRINCIPAL)

    async def test_a_provider_derives_for_the_principal_decided(self) -> None:
        member = _Provider(name="members", keys=frozenset({"ledger.write"}))
        await _grants(None, member)

        assert member.seen == [PRINCIPAL]


class TestTheOwnerOverride:
    async def test_a_derived_admin_key_overrides_ownership(self) -> None:
        admin = _Provider(
            name="admins",
            keys=frozenset({"invoice.read", "invoice.admin"}),
            granted=frozenset({"invoice.read", "invoice.admin"}),
        )
        grants = await _grants(None, admin)

        assert POLICY.decide(grants, _request("invoice.read", owner=uuid4())).allowed

    async def test_a_denied_admin_key_does_not(self) -> None:
        deps = _direct_binding("invoice.admin")
        reader = _Provider(
            name="members",
            keys=frozenset({"invoice.read", "invoice.admin"}),
            granted=frozenset({"invoice.read"}),
            denied=frozenset({"invoice.admin"}),
        )
        grants = await _grants(deps, reader)

        assert not POLICY.decide(grants, _request("invoice.read", owner=uuid4())).allowed


class TestTheResultType:
    @pytest.mark.parametrize(
        "keys", ["ledger.write", b"ledger.write", ["ledger.write", 7]], ids=["str", "bytes", "int"]
    )
    def test_a_key_set_is_a_collection_of_strings(self, keys: Any) -> None:
        # A string would otherwise become the set of its letters.
        for field in ("granted", "denied"):
            with pytest.raises(CoreException):
                DerivedPermissions(**{field: keys})

    def test_any_collection_of_strings_is_taken(self) -> None:
        derived = DerivedPermissions(granted=["a", "b"], denied=("c",))  # type: ignore[arg-type]

        assert (derived.granted, derived.denied) == (frozenset({"a", "b"}), frozenset({"c"}))


class TestTheDeclaration:
    @pytest.mark.parametrize(
        "providers",
        [
            (
                _Provider(name="a", keys=frozenset({"k"})),
                _Provider(name="a", keys=frozenset({"j"})),
            ),
            (_Provider(name=" ", keys=frozenset({"k"})),),
            (_Provider(name="a", keys=frozenset()),),
            (_Provider(name="a", keys="ledger.write"),),  # type: ignore[arg-type]
            (_Provider(name="a", keys=["ledger.write"]),),  # type: ignore[arg-type]
            (_Provider(name="a", keys=frozenset({"ledger.write", 7})),),  # type: ignore[arg-type]
        ],
        ids=[
            "duplicate-name",
            "blank-name",
            "no-keys",
            "bare-string-keys",
            "list-keys",
            "non-string-key",
        ],
    )
    def test_a_declaration_nobody_meant_is_refused(self, providers: Any) -> None:
        with pytest.raises(CoreException) as caught:
            AuthzKernelConfig(permission_providers=providers)

        assert caught.value.code == "authz_provider_declaration"

    def test_every_grant_consumer_runs_the_providers(self) -> None:
        # The decision, scope, grant-query and role-assignment adapters each build their own
        # resolver; each must carry the providers, or one of them decides on catalog grants only.
        from forze.application.contracts.authz import AuthzSpec
        from forze_identity.authz.execution.deps.deps import (
            ConfigurableAuthzDecision,
            ConfigurableAuthzScope,
            ConfigurableGrantQuery,
            ConfigurableRoleAssignment,
        )

        member = _Provider(name="members", keys=frozenset({"ledger.write"}))
        shared = build_authz_shared_services(AuthzKernelConfig(permission_providers=(member,)))
        ctx = context_from_modules(MockDepsModule())
        spec = AuthzSpec(name="main")

        for factory in (
            ConfigurableAuthzDecision(shared=shared),
            ConfigurableAuthzScope(shared=shared),
            ConfigurableGrantQuery(shared=shared),
            ConfigurableRoleAssignment(shared=shared),
        ):
            resolver = factory(ctx, spec).resolver  # type: ignore[attr-defined]
            assert resolver.providers == (member,), type(factory).__name__
            assert resolver.ctx is ctx


class TestTheBootCheck:
    async def _catalog(self, *keys: str) -> Any:
        ctx = context_from_modules(MockDepsModule())

        for key in keys:
            await ctx.doc.command(permission_definition_spec).create(
                CreatePermissionDefinitionCmd(permission_key=key)
            )

        return ctx

    async def test_declared_keys_the_catalog_defines_boot(self) -> None:
        ctx = await self._catalog("ledger.write", "ledger.read")
        member = _Provider(name="members", keys=frozenset({"ledger.write"}))

        await permission_providers_lifecycle_step([member]).startup(ctx)

    async def test_a_tenant_scoped_catalog_is_read_under_the_given_tenant(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from forze.application.contracts.tenancy import TenantIdentity
        from forze_identity.authz.execution import providers as boot

        tenant = TenantIdentity(tenant_id=uuid4())
        seen: list[Any] = []

        async def _fetch(qry: Any, *, filters: Any) -> list[Any]:
            seen.append(ctx.inv_ctx.get_tenant())
            return []

        ctx = context_from_modules(MockDepsModule())
        monkeypatch.setattr(boot, "fetch_all_document_hits", _fetch)
        member = _Provider(name="members", keys=frozenset({"ledger.write"}))

        with pytest.raises(CoreException):
            await permission_providers_lifecycle_step([member], tenant=tenant).startup(ctx)

        assert seen == [tenant]

    async def test_no_providers_boot_without_reading_the_catalog(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from forze_identity.authz.execution import providers as boot

        async def _fetch(qry: Any, *, filters: Any) -> list[Any]:
            raise AssertionError("the catalog was read for no keys")

        monkeypatch.setattr(boot, "fetch_all_document_hits", _fetch)

        await permission_providers_lifecycle_step([]).startup(
            context_from_modules(MockDepsModule())
        )

    async def test_a_key_the_catalog_lacks_refuses_to_boot(self) -> None:
        ctx = await self._catalog("ledger.write")
        member = _Provider(name="members", keys=frozenset({"ledger.write", "ledger.wrte"}))

        with pytest.raises(CoreException) as caught:
            await permission_providers_lifecycle_step([member]).startup(ctx)

        assert caught.value.code == "authz_provider_unknown_keys"
        assert "ledger.wrte" in caught.value.summary and "members" in caught.value.summary

    def test_the_step_refuses_a_declaration_the_config_would(self) -> None:
        member = _Provider(name="members", keys=["ledger.write"])  # type: ignore[arg-type]

        with pytest.raises(CoreException) as caught:
            permission_providers_lifecycle_step([member])

        assert caught.value.code == "authz_provider_declaration"
