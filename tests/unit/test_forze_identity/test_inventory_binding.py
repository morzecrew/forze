"""Identity spec binding: schema mapping, validation, and feature-group anti-drift.

The feature groups in ``forze_identity.inventory`` claim to name what the dependency
factories actually resolve. Claims like that rot silently, so each group is pinned here
by *running* the factory against a recording context and comparing the specs it asked
for — not by re-reading the factory's source.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.contracts.inventory import SpecSource, inventory_route_guard
from forze.application.execution import Deps, build_runtime
from forze.base.exceptions import CoreException, ExceptionKind
from forze_identity.inventory import (
    AUTHN_SPECS,
    AUTHZ_DECISION_SPECS,
    AUTHZ_SPECS,
    DELEGATION_SPECS,
    GRANT_RESOLUTION_SPECS,
    PASSWORD_LIFECYCLE_SPECS,
    TENANCY_SPECS,
    TENANT_RESOLUTION_SPECS,
    identity_document_names,
    spec_contributions,
)

pytestmark = pytest.mark.unit

_ALL_NAMES = {str(s.name) for s in (*AUTHN_SPECS, *AUTHZ_SPECS, *TENANCY_SPECS)}


def _names(specs: tuple[Any, ...]) -> set[str]:
    return {str(s.name) for s in specs}


# ....................... #
# The mapping helper


class TestIdentityDocumentNames:
    def test_default_names_every_spec(self) -> None:
        names = identity_document_names()

        assert set(names) == _ALL_NAMES
        assert len(names) == 19

    def test_groups_compose_and_deduplicate(self) -> None:
        # policy_principal appears in both groups; the selection holds it once.
        names = identity_document_names((*AUTHZ_DECISION_SPECS, *DELEGATION_SPECS))

        assert set(names) == _names(AUTHZ_DECISION_SPECS) | _names(DELEGATION_SPECS)
        assert len(names) == len(set(names))

    def test_accepts_bare_names(self) -> None:
        assert identity_document_names(["authn_token_sessions"]) == ("authn_token_sessions",)

    def test_unknown_name_is_refused_naming_it(self) -> None:
        with pytest.raises(CoreException, match="authn_sesions") as ei:
            identity_document_names(["authn_sesions"])

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_foreign_spec_object_is_refused(self) -> None:
        foreign = MagicMock()
        foreign.name = "authn_token_sessions"

        with pytest.raises(CoreException) as ei:
            identity_document_names([foreign])

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_unpacked_group_misuse_is_refused_cleanly(self) -> None:
        # A tuple-of-groups (missing the * unpack) must fail as configuration,
        # not as a raw AttributeError deep in the loop.
        with pytest.raises(CoreException, match="unpack groups") as ei:
            identity_document_names((GRANT_RESOLUTION_SPECS, DELEGATION_SPECS))  # type: ignore[arg-type]

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_bare_string_selection_is_one_name(self) -> None:
        # A str is itself an iterable of characters; it must mean one name.
        assert identity_document_names("authn_token_sessions") == ("authn_token_sessions",)

    def test_empty_selection_is_refused(self) -> None:
        with pytest.raises(CoreException, match="selection is empty") as ei:
            identity_document_names([])

        assert ei.value.kind is ExceptionKind.CONFIGURATION


# ....................... #
# Feature-group anti-drift: run the factory, record what it resolves.


class _RecordingDocs:
    def __init__(self, sink: set[str]) -> None:
        self._sink = sink

    def query(self, spec: Any) -> Any:
        return self._port(spec)

    def command(self, spec: Any) -> Any:
        return self._port(spec)

    def _port(self, spec: Any) -> Any:
        self._sink.add(str(spec.name))
        # Adapters validate their ports at construction (secure-spec rules, spec
        # identity checks), so the dummy must carry the real spec.
        port = MagicMock()
        port.spec = spec
        return port


class _RecordingCtx:
    def __init__(self) -> None:
        self.resolved: set[str] = set()
        self.doc = _RecordingDocs(self.resolved)
        self.document = self.doc
        self.deps = MagicMock()
        self.deps.exists.return_value = False
        self.deps.provide.return_value = lambda ctx, spec: MagicMock()
        self.inv_ctx = MagicMock()
        self.inv_ctx.get_tenant.return_value = None


class TestFeatureGroupsMatchTheFactories:
    def test_grant_resolution_group(self) -> None:
        from forze_identity.authz.execution.deps.deps import _grant_resolver

        ctx = _RecordingCtx()
        _grant_resolver(ctx)  # type: ignore[arg-type]

        assert ctx.resolved == _names(GRANT_RESOLUTION_SPECS)

    def test_authz_decision_group(self) -> None:
        from forze.application.contracts.authz import AuthzSpec
        from forze_identity.authz.execution.deps.deps import ConfigurableAuthzDecision

        ctx = _RecordingCtx()
        shared = MagicMock()
        ConfigurableAuthzDecision(shared=shared)(ctx, AuthzSpec(name="z"))  # type: ignore[arg-type]

        assert ctx.resolved == _names(AUTHZ_DECISION_SPECS)

    def test_delegation_group(self) -> None:
        from forze.application.contracts.authz import AuthzSpec
        from forze_identity.authz.execution.deps.deps import (
            ConfigurableDelegationGrant,
            ConfigurableDelegationQuery,
        )

        ctx = _RecordingCtx()
        ConfigurableDelegationGrant()(ctx, AuthzSpec(name="z"))  # type: ignore[arg-type]
        ConfigurableDelegationQuery()(ctx, AuthzSpec(name="z"))  # type: ignore[arg-type]

        assert ctx.resolved == _names(DELEGATION_SPECS)

    def test_password_lifecycle_group(self) -> None:
        from forze.application.contracts.authn import AuthnSpec
        from forze_identity.authn.execution.deps.deps import (
            ConfigurablePasswordAccountProvisioning,
            ConfigurablePasswordLifecycle,
            ConfigurablePasswordReset,
            ConfigurablePolicyPrincipalEligibility,
        )

        ctx = _RecordingCtx()
        shared = MagicMock()
        spec = AuthnSpec(name="r")

        ConfigurablePasswordLifecycle(shared=shared)(ctx, spec)  # type: ignore[arg-type]
        ConfigurablePasswordReset(shared=shared)(ctx, spec)  # type: ignore[arg-type]
        ConfigurablePasswordAccountProvisioning(shared=shared)(ctx, spec)  # type: ignore[arg-type]
        # The default eligibility gate runs on every flow and reads the policy
        # principal; the group must cover a default deployment, not just the
        # lifecycle adapters themselves.
        ConfigurablePolicyPrincipalEligibility()(ctx, spec)  # type: ignore[arg-type]

        assert ctx.resolved == _names(PASSWORD_LIFECYCLE_SPECS)

    def test_tenant_resolution_group(self) -> None:
        from forze_identity.tenancy.execution.deps.deps import (
            ConfigurableTenantManagement,
            ConfigurableTenantResolver,
        )

        ctx = _RecordingCtx()
        ConfigurableTenantResolver(verify_tenant_active=True)(ctx)  # type: ignore[arg-type]
        ConfigurableTenantManagement()(ctx)  # type: ignore[arg-type]

        assert ctx.resolved == _names(TENANT_RESOLUTION_SPECS)


# ....................... #
# The plane selection


class TestSpecContributionPlanes:
    """`spec_contributions(planes=…)` — cataloguing the planes an application wires.

    The reason this exists is not ergonomics: `build_runtime(specs=…)` refuses a spec that
    is catalogued and never bound, so an application wiring authn alone could not use the
    framework's own contribution helper and the reconciliation check together at all. One
    of the two had to be abandoned, and the helper was the one applications dropped.
    """

    def test_the_default_is_still_all_three_planes(self) -> None:
        """Pinned as an entry set, so adding a fourth plane shows up here as a diff.

        A count alone would let a plane be swapped for another silently.
        """

        entries = spec_contributions().freeze().entries

        assert {entry.name for entry in entries} == _ALL_NAMES
        assert len(entries) == 19
        assert all(entry.source is SpecSource.FRAMEWORK for entry in entries)
        assert all(entry.identity for entry in entries)

    @pytest.mark.parametrize(
        ("plane", "specs"),
        [("authn", AUTHN_SPECS), ("authz", AUTHZ_SPECS), ("tenancy", TENANCY_SPECS)],
    )
    def test_one_plane_returns_exactly_that_plane(
        self,
        plane: str,
        specs: tuple[Any, ...],
    ) -> None:
        entries = spec_contributions(planes=[plane]).freeze().entries  # type: ignore[list-item]

        assert {entry.name for entry in entries} == _names(specs)

    def test_two_planes_compose(self) -> None:
        entries = spec_contributions(planes=["authn", "tenancy"]).freeze().entries

        assert {entry.name for entry in entries} == _names(AUTHN_SPECS) | _names(TENANCY_SPECS)

    def test_a_repeated_plane_is_not_registered_twice(self) -> None:
        """Registering one spec twice is refused outright by the registry's metadata check,
        so a duplicated selection must deduplicate before it gets there."""

        entries = spec_contributions(planes=["authn", "authn"]).freeze().entries

        assert {entry.name for entry in entries} == _names(AUTHN_SPECS)
        assert len(entries) == len(AUTHN_SPECS)

    def test_the_order_of_the_selection_does_not_change_the_result(self) -> None:
        """The registry is a set of entries; a fingerprint must not depend on spelling."""

        forward = spec_contributions(planes=["authn", "tenancy"]).freeze()
        backward = spec_contributions(planes=["tenancy", "authn"]).freeze()

        assert [e.name for e in forward.entries] == [e.name for e in backward.entries]

    def test_an_empty_selection_is_refused(self) -> None:
        """Not read as "nothing": an emptied constant or a bad comprehension fails here."""

        with pytest.raises(CoreException, match="selection is empty") as ei:
            spec_contributions(planes=[])

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_an_unknown_plane_is_refused_and_names_the_three(self) -> None:
        with pytest.raises(CoreException, match="authz") as ei:
            spec_contributions(planes=["authorization"])  # type: ignore[list-item]

        assert "authorization" in str(ei.value)
        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_an_unknown_plane_beside_a_known_one_is_still_refused(self) -> None:
        """The selection is validated whole; a typo does not pass by having company."""

        with pytest.raises(CoreException, match="nope"):
            spec_contributions(planes=["authn", "nope"])  # type: ignore[list-item]


# ....................... #
# What the selection is for: the reconciliation check becomes usable


def _bound_documents(*names: str) -> Deps:
    """Deps binding each *name* as a routed document, the way a wired plane appears."""

    return Deps.routed(
        {
            DocumentQueryDepKey: dict.fromkeys(names, object()),
            DocumentCommandDepKey: dict.fromkeys(names, object()),
        }
    )


_AUTHN_NAMES = identity_document_names(AUTHN_SPECS)


class TestReconciliationWithAPartialPlane:
    """The motivation, as a test: two shipped features that could not be used together.

    `build_runtime(specs=…)` refuses a spec catalogued but never bound, and the framework's
    own helper catalogued all three planes. An application wiring authn alone had to give
    up one of the two, and it gave up the helper — which is how a feature's adoption goes
    to zero without a bug report.
    """

    def test_an_authn_only_application_reconciles_with_the_narrowed_helper(self) -> None:
        runtime = build_runtime(
            deps=[_bound_documents(*_AUTHN_NAMES)],
            specs=spec_contributions(planes=["authn"]),
        )

        assert runtime.spec_registry is not None
        assert {e.name for e in runtime.spec_registry.entries} == set(_AUTHN_NAMES)

    def test_the_unnarrowed_helper_is_what_refused(self) -> None:
        """The failure this removes, pinned so the test above cannot pass vacuously.

        Thirteen documents, not twelve: the eleven authz specs and the two tenancy ones.
        """

        with pytest.raises(CoreException, match="no dependency binds it") as ei:
            build_runtime(
                deps=[_bound_documents(*_AUTHN_NAMES)],
                specs=spec_contributions(),
            )

        unbound = {name for name in _ALL_NAMES if f"document:{name}: catalogued" in str(ei.value)}

        assert unbound == _names(AUTHZ_SPECS) | _names(TENANCY_SPECS)
        assert len(unbound) == 13

    def test_a_narrowed_selection_still_fails_from_the_bound_side(self) -> None:
        """§5.2: narrowing does not buy silence about a document the plane really binds.

        An authn plane running the eligibility check reads `authz_policy_principals`. Select
        `["authn"]` with that check live and the document is bound and uncatalogued — the
        mirror failure, and the message has to point at the selection or the author is
        handed a worse error than the one the selection removed.
        """

        with pytest.raises(CoreException, match="missing from the spec inventory") as ei:
            build_runtime(
                deps=[_bound_documents(*_AUTHN_NAMES, "authz_policy_principals")],
                specs=spec_contributions(planes=["authn"]),
            )

        message = str(ei.value)

        assert "authz_policy_principals" in message
        assert "narrowed to a subset" in message

    def test_the_hint_reaches_the_resolve_time_refusal_too(self) -> None:
        """The same failure has two refusals, and a routeless provider only meets one.

        `reconcile_specs` compares registrations, so a plain provider — the mock's shape and
        most production wiring — carries no route to compare and is refused at resolve time
        by the route guard instead. A hint on the registration message alone would leave
        that shape with nothing.
        """

        registry = spec_contributions(planes=["authn"]).freeze()
        guard = inventory_route_guard(registry)

        guard(DocumentQueryDepKey.name, "authn_token_sessions")

        with pytest.raises(CoreException, match="narrowed to a subset") as ei:
            guard(DocumentQueryDepKey.name, "authz_policy_principals")

        assert ei.value.kind is ExceptionKind.CONFIGURATION
