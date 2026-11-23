"""``eligibility="allow_all"`` warns when constructed and refuses under a production posture."""

from __future__ import annotations

from enum import Enum
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.deps import Deps
from forze.application.contracts.document import DocumentQueryDepKey
from forze.application.execution import Exempt, ProductionPosture, build_runtime
from forze.base.exceptions import CoreException
from forze_identity.authn import AuthnDepsModule, AuthnKernelConfig
from forze_identity.authn.execution.deps import module as authn_module
from forze_identity.authz import policy_principal_spec

pytestmark = pytest.mark.unit

# ----------------------- #


class _Settings(BaseModel):
    env: str | None = None


class _Routes(str, Enum):
    MAIN = "main"


def _posture(env: str | None, *exempt: Exempt) -> ProductionPosture:
    return ProductionPosture(
        settings=_Settings(env=env),
        environment="env",
        non_production=frozenset({"dev", "test"}),
        exempt=exempt,
    )


def _module(eligibility: Any = "allow_all") -> AuthnDepsModule:
    # Two routes: one authenticates, the other only manages token lifecycle.
    return AuthnDepsModule(
        kernel=AuthnKernelConfig(access_token_secret=b"k" * 32, refresh_token_pepper=b"p" * 32),
        authn={"main": frozenset({"token"})},
        token_lifecycle={"admin"},
        eligibility=eligibility,
    )


@pytest.fixture
def warnings(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    # The authn logger is structlog-backed and writes past caplog, so it is patched.
    spy = MagicMock()
    monkeypatch.setattr(authn_module, "logger", spy)

    return spy


# ....................... #


class TestTheFinding:
    def test_one_per_route_and_none_for_any_other_gate(self) -> None:
        assert [f.render() for f in _module().posture_findings()] == [
            f"authn_eligibility:{route}: {authn_module.ALLOW_ALL_RULE}"
            for route in ("admin", "main")
        ]
        assert _module("policy_principal").posture_findings() == ()
        assert _module(lambda ctx, spec: None).posture_findings() == ()

    def test_the_routes_are_the_ones_given_at_construction(self) -> None:
        # Freezing reads the findings before it builds the module's Deps; a caller's set
        # changed in between moves neither.
        routes = {"admin"}
        module = AuthnDepsModule(
            kernel=AuthnKernelConfig(access_token_secret=b"k" * 32, refresh_token_pepper=b"p" * 32),
            authn={"main": frozenset({"token"})},
            token_lifecycle=routes,
            eligibility="allow_all",
        )
        routes.add("late")

        assert [f.target for f in module.posture_findings()] == [
            "authn_eligibility:admin",
            "authn_eligibility:main",
        ]
        assert all("late" not in registered for registered in module().routed_deps.values())

    def test_an_enum_route_is_named_by_its_value(self, warnings: MagicMock) -> None:
        # str() of a (str, Enum) member is "_Routes.MAIN"; the Exempt a deployment writes
        # names the route it registered, "main".
        module = AuthnDepsModule(
            kernel=AuthnKernelConfig(access_token_secret=b"k" * 32),
            authn={_Routes.MAIN: frozenset({"token"})},
            eligibility="allow_all",
        )

        assert [f.target for f in module.posture_findings()] == ["authn_eligibility:main"]
        assert warnings.warning.call_args.kwargs["routes"] == ["main"]

        exempt = Exempt(target="authn_eligibility:main", reason="r")
        build_runtime(module, posture=_posture("production", exempt))
        build_runtime(deps=module(), posture=_posture("production", exempt))


class TestUnderAPosture:
    @pytest.mark.parametrize("env", [None, "production"], ids=["unset", "production"])
    def test_production_refuses_the_boot(self, env: str | None) -> None:
        with pytest.raises(CoreException) as caught:
            build_runtime(_module(), posture=_posture(env))

        assert caught.value.code == "production_posture_refused"
        assert "authn_eligibility:main: admits every principal" in caught.value.summary
        assert "authn_eligibility:admin: admits every principal" in caught.value.summary

    def test_an_exemption_for_every_route_lets_it_boot(self) -> None:
        reason = "No authorization plane yet"
        exempt = (
            Exempt(target="authn_eligibility:main", reason=reason),
            Exempt(target="authn_eligibility:admin", reason=reason),
        )

        build_runtime(_module(), posture=_posture("production", *exempt))

    def test_an_exemption_covers_only_its_route(self) -> None:
        main = Exempt(target="authn_eligibility:main", reason="No authorization plane yet")

        with pytest.raises(CoreException) as caught:
            build_runtime(_module(), posture=_posture("production", main))

        assert "authn_eligibility:admin" in caught.value.summary
        assert "authn_eligibility:main" not in caught.value.summary

    def test_the_default_gate_boots_in_production(self) -> None:
        store = Deps.routed(
            {DocumentQueryDepKey: {policy_principal_spec.name: lambda ctx, spec: MagicMock()}}
        )

        build_runtime(_module("policy_principal"), deps=store, posture=_posture("production"))


class TestTheWarning:
    @pytest.mark.parametrize("env", [None, "dev"], ids=["no-posture", "non-production"])
    def test_outside_production_it_only_warns(self, env: str | None, warnings: MagicMock) -> None:
        build_runtime(_module(), posture=None if env is None else _posture(env))

        warnings.warning.assert_called_once()
        assert warnings.warning.call_args.args == ("authn.eligibility.allow_all",)
        assert warnings.warning.call_args.kwargs["routes"] == ["admin", "main"]

    def test_built_deps_warn_too(self, warnings: MagicMock) -> None:
        # Construction runs however the module is passed, so wiring that hands over its
        # built Deps, as the presets do, still says it checks nobody.
        build_runtime(deps=_module()())

        warnings.warning.assert_called_once()

    def test_no_other_gate_warns(self, warnings: MagicMock) -> None:
        _module("policy_principal")
        _module(lambda ctx, spec: None)

        warnings.warning.assert_not_called()
