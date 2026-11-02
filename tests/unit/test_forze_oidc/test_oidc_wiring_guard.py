"""Wiring guard: every external OIDC verifier factory needs an explicit principal resolver.

The guard in ``AuthnDepsModule`` refuses a ``token_verifiers`` override without a
``resolvers`` override, because the default ``JwtNativeUuidResolver`` would take an external
subject as an internal principal UUID. It is type-agnostic; these cases pin that the new
introspection and whoami factories fall under it like the JWKS one, and wire when paired.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

pytest.importorskip("jwt")

from forze.application.contracts.authn import (
    AuthnDepKey,
    AuthnSpec,
    TokenVerifierDepKey,
    TokenVerifierDepPort,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_deps
from forze_identity.authn import (
    AuthnDepsModule,
    AuthnKernelConfig,
    ConfigurableDeterministicUuidResolver,
)
from forze_identity.builtin.idp._oidc import oidc_bootstrap_identity_deps
from forze_identity.oidc import (
    BasicClientAuth,
    ConfigurableIntrospectionVerifier,
    ConfigurableOidcIdpVerifier,
    ConfigurableWhoamiVerifier,
    IntrospectionConfig,
    IntrospectionTokenVerifier,
    OidcIdpPreset,
    OidcTokenVerifier,
    WhoamiConfig,
    WhoamiSessionVerifier,
)

pytestmark = pytest.mark.unit

ISSUER = "https://idp.example"
SPEC = AuthnSpec(name="api", enabled_methods=frozenset({"token"}))

FACTORIES: list[tuple[Callable[[], TokenVerifierDepPort], type[Any]]] = [
    (
        lambda: ConfigurableOidcIdpVerifier(
            preset=OidcIdpPreset(issuer=ISSUER, jwks_uri=f"{ISSUER}/jwks", audience="api"),
        ),
        OidcTokenVerifier,
    ),
    (
        lambda: ConfigurableIntrospectionVerifier(
            config=IntrospectionConfig(
                endpoint=f"{ISSUER}/introspect",
                issuer=ISSUER,
                audience="api",
                client_auth=BasicClientAuth(client_id="rs", client_secret="rs-pass"),
            ),
        ),
        IntrospectionTokenVerifier,
    ),
    (
        lambda: ConfigurableWhoamiVerifier(
            config=WhoamiConfig(
                endpoint=f"{ISSUER}/sessions/whoami",
                issuer=ISSUER,
                credential_style="cookie",
            ),
        ),
        WhoamiSessionVerifier,
    ),
]

IDS = ["jwks", "introspection", "whoami"]

# ----------------------- #


@pytest.mark.parametrize(("build", "_kind"), FACTORIES, ids=IDS)
def test_external_verifier_without_resolver_is_refused(
    build: Callable[[], TokenVerifierDepPort],
    _kind: type[Any],
) -> None:
    with pytest.raises(CoreException, match="resolvers override") as caught:
        AuthnDepsModule(
            kernel=AuthnKernelConfig(),
            authn={"api": frozenset({"token"})},
            token_verifiers={"api": build()},
            eligibility="allow_all",
        )()

    assert caught.value.kind is ExceptionKind.CONFIGURATION


@pytest.mark.parametrize(("build", "kind"), FACTORIES, ids=IDS)
def test_external_verifier_with_resolver_wires_and_resolves(
    build: Callable[[], TokenVerifierDepPort],
    kind: type[Any],
) -> None:
    deps = AuthnDepsModule(
        kernel=AuthnKernelConfig(),
        authn={"api": frozenset({"token"})},
        token_verifiers={"api": build()},
        resolvers={"api": ConfigurableDeterministicUuidResolver()},
        eligibility="allow_all",
    )()
    ctx = context_from_deps(deps)

    verifier = ctx.deps.provide(TokenVerifierDepKey, route="api")(ctx, SPEC)

    assert isinstance(verifier, kind)
    assert deps.exists(AuthnDepKey, route="api")


@pytest.mark.parametrize(("build", "_kind"), FACTORIES, ids=IDS)
def test_bootstrap_deps_pair_every_verifier_with_a_resolver(
    build: Callable[[], TokenVerifierDepPort],
    _kind: type[Any],
) -> None:
    deps = oidc_bootstrap_identity_deps(authn_route="api", token_verifier=build())

    assert deps.exists(AuthnDepKey, route="api")
