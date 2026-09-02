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
        )

        ctx = _RecordingCtx()
        shared = MagicMock()
        spec = AuthnSpec(name="r")

        ConfigurablePasswordLifecycle(shared=shared)(ctx, spec)  # type: ignore[arg-type]
        ConfigurablePasswordReset(shared=shared)(ctx, spec)  # type: ignore[arg-type]
        ConfigurablePasswordAccountProvisioning(shared=shared)(ctx, spec)  # type: ignore[arg-type]

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
