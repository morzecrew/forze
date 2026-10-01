"""Config grants: a key the configuration owns comes from the configuration and nowhere else.

The catalog legs run against the mock's real catalog documents, through the resolver and the
policy, because the overlap takes a different path per binding kind (role, principal, group): a
refusal that reads one binding collection and not the others is what a hand-built snapshot would
never show. The runtime leg writes its binding after the first decision, past the one-shot check,
which is the case the provider's own denial exists for.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
import structlog
from pydantic import ValidationError

from forze.application.contracts.authz import (
    AuthzRequest,
    AuthzSubject,
    DerivedPermissionRef,
    PermissionProvider,
)
from forze.base.exceptions import CoreException
from forze.testing import context_from_modules
from forze_identity.authz import (
    AuthzKernelConfig,
    ConfigGrants,
    ConfigGrantsProvider,
    build_authz_shared_services,
    permission_providers_lifecycle_step,
)
from forze_identity.authz.application.specs import (
    group_permission_binding_spec,
    group_principal_binding_spec,
    group_spec,
    permission_definition_spec,
    principal_permission_binding_spec,
    principal_role_binding_spec,
    role_definition_spec,
    role_permission_binding_spec,
)
from forze_identity.authz.domain.models.bindings import (
    CreateGroupPermissionBindingCmd,
    CreateGroupPrincipalBindingCmd,
    CreatePrincipalPermissionBindingCmd,
    CreatePrincipalRoleBindingCmd,
    CreateRolePermissionBindingCmd,
)
from forze_identity.authz.domain.models.group import CreateGroupCmd
from forze_identity.authz.domain.models.permission_definition import (
    CreatePermissionDefinitionCmd,
)
from forze_identity.authz.domain.models.role_definition import CreateRoleDefinitionCmd
from forze_identity.authz.execution.deps.deps import (
    _grant_resolver,  # pyright: ignore[reportPrivateUsage]
)
from forze_identity.authz.services.policy import AuthzPolicyService
from forze_mock import MockDepsModule

pytestmark = pytest.mark.unit

BREAK_GLASS = "ops.break_glass"
OPERATOR = uuid4()
SOMEONE = uuid4()
POLICY = AuthzPolicyService()


def _config(**grants: Any) -> ConfigGrantsProvider:
    return ConfigGrantsProvider(keys=frozenset(grants), grants=ConfigGrants(grants=grants))


async def _catalog(*keys: str) -> tuple[Any, dict[str, UUID]]:
    ctx = context_from_modules(MockDepsModule())
    ids: dict[str, UUID] = {}

    for key in keys:
        row = await ctx.doc.command(permission_definition_spec).create(
            CreatePermissionDefinitionCmd(permission_key=key)
        )
        ids[key] = row.id

    return ctx, ids


async def _bind_through_role(ctx: Any, permission_id: UUID, principal_id: UUID) -> None:
    role = await ctx.doc.command(role_definition_spec).create(
        CreateRoleDefinitionCmd(role_key=f"role-{uuid4().hex[:6]}")
    )
    await ctx.doc.command(role_permission_binding_spec).create(
        CreateRolePermissionBindingCmd(role_id=role.id, permission_id=permission_id)
    )
    await ctx.doc.command(principal_role_binding_spec).create(
        CreatePrincipalRoleBindingCmd(principal_id=principal_id, role_id=role.id)
    )


async def _bind_directly(ctx: Any, permission_id: UUID, principal_id: UUID) -> None:
    await ctx.doc.command(principal_permission_binding_spec).create(
        CreatePrincipalPermissionBindingCmd(principal_id=principal_id, permission_id=permission_id)
    )


async def _bind_through_group(ctx: Any, permission_id: UUID, principal_id: UUID) -> None:
    group = await ctx.doc.command(group_spec).create(
        CreateGroupCmd(group_key=f"group-{uuid4().hex[:6]}")
    )
    await ctx.doc.command(group_permission_binding_spec).create(
        CreateGroupPermissionBindingCmd(group_id=group.id, permission_id=permission_id)
    )
    await ctx.doc.command(group_principal_binding_spec).create(
        CreateGroupPrincipalBindingCmd(group_id=group.id, principal_id=principal_id)
    )


BINDINGS = [
    pytest.param(_bind_through_role, id="role"),
    pytest.param(_bind_directly, id="principal"),
    pytest.param(_bind_through_group, id="group"),
]


async def _allowed(ctx: Any, providers: tuple[PermissionProvider, ...], who: UUID, key: str) -> bool:
    shared = build_authz_shared_services(AuthzKernelConfig(permission_providers=providers))
    grants = await _grant_resolver(ctx, shared).resolve_effective_grants(who)
    request = AuthzRequest(subject=AuthzSubject(principal_id=who), action=key)

    return POLICY.decide(grants, request).allowed


# ....................... #


class TestTheConfigurationGrants:
    async def test_a_listed_principal_holds_the_key_and_nobody_else_does(self) -> None:
        ctx, _ = await _catalog(BREAK_GLASS)
        providers = (_config(**{BREAK_GLASS: [OPERATOR]}),)

        assert await _allowed(ctx, providers, OPERATOR, BREAK_GLASS)
        assert not await _allowed(ctx, providers, SOMEONE, BREAK_GLASS)

    async def test_a_key_listed_with_nobody_is_granted_to_nobody(self) -> None:
        ctx, _ = await _catalog(BREAK_GLASS)

        assert not await _allowed(ctx, (_config(**{BREAK_GLASS: []}),), OPERATOR, BREAK_GLASS)

    async def test_the_grant_is_attributed_to_the_configuration_not_the_catalog(self) -> None:
        ctx, _ = await _catalog(BREAK_GLASS)
        providers = (_config(**{BREAK_GLASS: [OPERATOR]}),)
        shared = build_authz_shared_services(AuthzKernelConfig(permission_providers=providers))
        resolver = _grant_resolver(ctx, shared)

        held = await resolver.resolve_effective_grants(OPERATOR)
        refused = await resolver.resolve_effective_grants(SOMEONE)

        assert held.derived == {DerivedPermissionRef(permission_key=BREAK_GLASS, provider="config")}
        assert not held.permissions
        assert refused.derived == {
            DerivedPermissionRef(permission_key=BREAK_GLASS, provider="config", denied=True)
        }

    @pytest.mark.parametrize("bind", BINDINGS)
    async def test_a_binding_written_after_the_check_grants_nothing(self, bind: Any) -> None:
        # The overlap check runs once per tenant; an admin binding the key after it must not
        # hand out a break-glass permission until the next restart.
        ctx, ids = await _catalog(BREAK_GLASS)
        providers = (_config(**{BREAK_GLASS: [OPERATOR]}),)
        shared = build_authz_shared_services(AuthzKernelConfig(permission_providers=providers))
        resolver = _grant_resolver(ctx, shared)

        await resolver.resolve_effective_grants(OPERATOR)
        await bind(ctx, ids[BREAK_GLASS], SOMEONE)
        grants = await resolver.resolve_effective_grants(SOMEONE)

        assert BREAK_GLASS in {ref.permission_key for ref in grants.permissions}
        assert not POLICY.decide(
            grants, AuthzRequest(subject=AuthzSubject(principal_id=SOMEONE), action=BREAK_GLASS)
        ).allowed


class TestAnOverlapWithTheCatalog:
    @pytest.mark.parametrize("bind", BINDINGS)
    async def test_at_boot(self, bind: Any) -> None:
        ctx, ids = await _catalog(BREAK_GLASS)
        await bind(ctx, ids[BREAK_GLASS], SOMEONE)
        step = permission_providers_lifecycle_step([_config(**{BREAK_GLASS: [OPERATOR]})])

        with pytest.raises(CoreException, match=BREAK_GLASS) as caught:
            await step.startup(ctx)  # type: ignore[misc]

        assert caught.value.code == "authz_config_grant_overlap"

    @pytest.mark.parametrize("bind", BINDINGS)
    async def test_the_first_decision_logs_it_and_still_decides(self, bind: Any) -> None:
        # Refusing here would let anyone who may write a binding stop every decision in the
        # tenant; the provider's denial already keeps the binding from granting anything.
        ctx, ids = await _catalog(BREAK_GLASS)
        await bind(ctx, ids[BREAK_GLASS], SOMEONE)
        providers = (_config(**{BREAK_GLASS: [OPERATOR]}),)

        with structlog.testing.capture_logs() as logs:
            operator = await _allowed(ctx, providers, OPERATOR, BREAK_GLASS)

        assert (operator, await _allowed(ctx, providers, SOMEONE, BREAK_GLASS)) == (True, False)
        assert [log["keys"] for log in logs if log["event"] == "authz.config_grant_overlap"] == [
            [BREAK_GLASS]
        ]

    async def test_it_is_logged_once_per_tenant_not_on_every_decision(self) -> None:
        # The check is memoised per tenant whether or not it found an overlap: a log line and four
        # catalog reads on every decision would be the cost of a binding nobody may use.
        ctx, ids = await _catalog(BREAK_GLASS)
        await _bind_directly(ctx, ids[BREAK_GLASS], SOMEONE)
        providers = (_config(**{BREAK_GLASS: [OPERATOR]}),)
        shared = build_authz_shared_services(AuthzKernelConfig(permission_providers=providers))
        resolver = _grant_resolver(ctx, shared)

        with structlog.testing.capture_logs() as logs:
            for who in (OPERATOR, SOMEONE, OPERATOR):
                await resolver.resolve_effective_grants(who)

        assert [log["event"] for log in logs].count("authz.config_grant_overlap") == 1

    async def test_a_key_defined_but_bound_nowhere_boots(self) -> None:
        ctx, _ = await _catalog(BREAK_GLASS)
        step = permission_providers_lifecycle_step([_config(**{BREAK_GLASS: [OPERATOR]})])

        await step.startup(ctx)  # type: ignore[misc]

    async def test_a_key_the_catalog_does_not_define_is_refused(self) -> None:
        ctx, _ = await _catalog()
        step = permission_providers_lifecycle_step([_config(**{BREAK_GLASS: [OPERATOR]})])

        with pytest.raises(CoreException) as caught:
            await step.startup(ctx)  # type: ignore[misc]

        assert caught.value.code == "authz_provider_unknown_keys"

    async def test_another_providers_bound_key_is_no_overlap(self) -> None:
        # Only a key the configuration owns has one source; a document-derived key sits beside
        # catalog bindings by design.
        class _Members:
            name = "members"
            keys = frozenset({"ledger.write"})

            async def derive(self, principal_id: UUID, ctx: Any) -> Any:
                raise AssertionError("not reached at boot")

        ctx, ids = await _catalog(BREAK_GLASS, "ledger.write")
        await _bind_directly(ctx, ids["ledger.write"], SOMEONE)
        step = permission_providers_lifecycle_step(
            [_Members(), _config(**{BREAK_GLASS: [OPERATOR]})]
        )

        await step.startup(ctx)  # type: ignore[misc]


class _Members:
    """A document-derived provider, declaring a key of its own."""

    name = "members"

    def __init__(self, *keys: str) -> None:
        self.keys = frozenset(keys)

    async def derive(self, principal_id: UUID, ctx: Any) -> Any:
        raise AssertionError("not reached at declaration")


class TestAKeyHasOneProvider:
    @pytest.mark.parametrize(
        "other",
        [
            pytest.param(lambda: _Members(BREAK_GLASS), id="another-provider"),
            pytest.param(
                lambda: ConfigGrantsProvider(
                    name="second",
                    keys=frozenset({BREAK_GLASS}),
                    grants=ConfigGrants(grants={BREAK_GLASS: frozenset({SOMEONE})}),
                ),
                id="second-configuration",
            ),
        ],
    )
    def test_a_config_key_another_provider_declares_is_refused(self, other: Any) -> None:
        # The configuration denies the key to everyone it does not list: the other provider's
        # grants of it would never count.
        providers = (_config(**{BREAK_GLASS: [OPERATOR]}), other())

        with pytest.raises(CoreException, match=BREAK_GLASS) as kernel:
            AuthzKernelConfig(permission_providers=providers)

        with pytest.raises(CoreException, match=BREAK_GLASS) as step:
            permission_providers_lifecycle_step(providers)

        with pytest.raises(CoreException, match=BREAK_GLASS) as mock:
            MockDepsModule(permission_providers=providers)

        assert {kernel.value.code, step.value.code, mock.value.code} == {
            "authz_provider_declaration"
        }

    def test_distinct_keys_declare(self) -> None:
        AuthzKernelConfig(
            permission_providers=(_config(**{BREAK_GLASS: [OPERATOR]}), _Members("ledger.write"))
        )

    async def test_the_provider_reads_the_configuration_once(self) -> None:
        # Settings validate to a dict; nothing holding them may add a principal after the check.
        grants = ConfigGrants(grants={BREAK_GLASS: frozenset({OPERATOR})})
        provider = ConfigGrantsProvider(keys=frozenset({BREAK_GLASS}), grants=grants)

        grants.grants[BREAK_GLASS] = frozenset({SOMEONE})  # type: ignore[index]
        derived = await provider.derive(SOMEONE, None)  # type: ignore[arg-type]

        assert (derived.granted, derived.denied) == (frozenset(), frozenset({BREAK_GLASS}))


class TestTheKeysAreCode:
    def test_a_configuration_naming_an_undeclared_key_is_refused(self) -> None:
        # A deployment lists who holds a key, never which keys configuration owns.
        grants = ConfigGrants(grants={"ledger.write": frozenset({SOMEONE})})

        with pytest.raises(CoreException, match="ledger.write") as caught:
            ConfigGrantsProvider(keys=frozenset({BREAK_GLASS}), grants=grants)

        assert caught.value.code == "authz_provider_declaration"

    async def test_a_declared_key_the_configuration_omits_is_granted_to_nobody(self) -> None:
        provider = ConfigGrantsProvider(keys=frozenset({BREAK_GLASS}))

        derived = await provider.derive(OPERATOR, None)  # type: ignore[arg-type]

        assert (derived.granted, derived.denied) == (frozenset(), frozenset({BREAK_GLASS}))

    def test_keys_as_a_bare_string_are_refused(self) -> None:
        with pytest.raises(CoreException, match="frozenset"):
            ConfigGrantsProvider(keys=BREAK_GLASS)  # type: ignore[arg-type]


class TestTheSettingsModel:
    def test_it_loads_from_the_json_an_environment_variable_carries(self) -> None:
        grants = ConfigGrants.model_validate_json(f'{{"grants": {{"{BREAK_GLASS}": ["{OPERATOR}"]}}}}')

        assert grants.grants == {BREAK_GLASS: frozenset({OPERATOR})}

    @pytest.mark.parametrize(
        "grants",
        [
            pytest.param({BREAK_GLASS: ["ops-*"]}, id="pattern"),
            pytest.param({BREAK_GLASS: ["alice@example.com"]}, id="email"),
            pytest.param({" ": [str(OPERATOR)]}, id="blank-key"),
        ],
    )
    def test_anything_but_exact_principal_ids_under_named_keys_is_refused(
        self, grants: dict[str, list[str]]
    ) -> None:
        with pytest.raises(ValidationError):
            ConfigGrants(grants=grants)  # type: ignore[arg-type]

    def test_a_configuration_owning_no_keys_is_refused_as_a_provider(self) -> None:
        with pytest.raises(CoreException, match="declares no keys"):
            AuthzKernelConfig(permission_providers=(_config(),))
