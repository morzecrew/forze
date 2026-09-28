"""A production posture refuses a deployment that would run with development settings.

It reads the application's own settings by path, and it refuses at two moments — the wiring
check an application may call, and the runtime's construction it cannot skip. The leg that
matters most is the one about what a refusal *does not* say: a boot failure is logged, exported
and pasted into a ticket, so a secret planted in the settings must appear in none of it.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict, SecretStr

from forze.application.execution import (
    DEFAULT_RULES,
    POSTURE_REFUSED,
    DevValue,
    Exempt,
    LoopbackHost,
    ProductionPosture,
    RequireHttps,
    RequireSet,
    build_runtime,
    check_facade_factory_wiring,
    check_wiring,
)
from forze.application.execution.operations.facade import OperationFacade, OperationFacadeFactory
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException, ExceptionKind
from forze_http.execution.deps.configs import HttpAuthConfig, HttpServiceConfig
from forze_http.execution.deps.module import HttpDepsModule
from forze_http.kernel.client import HttpClient
from tests.support.execution_context import context_from_modules

# ----------------------- #

SECRET = "hunter2-SECRET-9f3c"


class _Db(BaseModel):
    dsn: str | None = None
    password: SecretStr | None = None


class _Http(BaseModel):
    public_base_url: str | None = None


class _Cors(BaseModel):
    allow_origins: list[str] = []


class _Settings(BaseModel):
    env: str | None = None
    db: _Db = _Db()
    http: _Http = _Http()
    cors: _Cors = _Cors()
    extras: dict[str, str] = {}


def _good(**overrides: Any) -> _Settings:
    base: dict[str, Any] = {
        "env": "production",
        "db": _Db(dsn="postgresql://app@db.internal:5432/app", password=SecretStr(SECRET)),
        "http": _Http(public_base_url="https://app.example.com"),
        "cors": _Cors(allow_origins=["https://app.example.com"]),
    }
    base.update(overrides)
    return _Settings(**base)


RULES = (
    *DEFAULT_RULES,
    RequireSet(fields=("db.password", "db.dsn")),
    RequireHttps(fields=("http.public_base_url", "cors.allow_origins")),
    LoopbackHost(fields=("db.dsn", "http.public_base_url")),
    DevValue(pattern="*", fields=("cors.allow_origins",)),
)


def _posture(settings: _Settings, **kwargs: Any) -> ProductionPosture:
    return ProductionPosture(
        settings=settings,
        environment="env",
        non_production=frozenset({"dev", "test"}),
        rules=RULES,
        **kwargs,
    )


def _refusals(settings: _Settings, **kwargs: Any) -> list[str]:
    return [finding.render() for finding in _posture(settings, **kwargs).findings()]


# ....................... #


class TestEachRuleKind:
    def test_good_settings_pass(self) -> None:
        assert _refusals(_good()) == []

    @pytest.mark.parametrize(
        "db",
        [
            _Db(dsn="postgresql://app@db.internal/app", password=None),
            _Db(dsn="postgresql://app@db.internal/app", password=SecretStr("  ")),
        ],
        ids=["none", "blank-secret"],
    )
    def test_an_unset_required_field_refuses(self, db: _Db) -> None:
        assert _refusals(_good(db=db)) == ["db.password: required and unset"]

    def test_an_http_url_where_https_is_required_refuses(self) -> None:
        refusals = _refusals(_good(http=_Http(public_base_url="http://app.example.com")))

        assert refusals == ["http.public_base_url: must be https"]

    def test_every_element_of_a_collection_must_be_https(self) -> None:
        cors = _Cors(allow_origins=["https://a.example.com", "http://b.example.com"])

        assert _refusals(_good(cors=cors)) == ["cors.allow_origins: must be https"]

    def test_a_wildcard_origin_refuses(self) -> None:
        refusals = _refusals(_good(cors=_Cors(allow_origins=["https://*.example.com"])))

        assert refusals == ["cors.allow_origins: development-only value (contains '*')"]

    @pytest.mark.parametrize(
        "dsn",
        [
            "postgresql://app@localhost:5432/app",
            "postgresql://app@127.0.0.1/app",
            "postgresql://app@[::1]/app",
            "localhost:5432",
        ],
    )
    def test_a_loopback_host_refuses(self, dsn: str) -> None:
        refusals = _refusals(_good(db=_Db(dsn=dsn, password=SecretStr(SECRET))))

        assert refusals == ["db.dsn: points at a loopback host"]

    def test_the_default_marker_is_found_anywhere(self) -> None:
        # No path: the shipped rule reads every string field, secrets and mappings included.
        settings = _good(
            db=_Db(dsn="postgresql://app@db.internal/app", password=SecretStr("pw_dev_only")),
            extras={"note": "seeded_dev_only"},
        )

        assert sorted(_refusals(settings)) == [
            "db.password: development-only value (contains '_dev_only')",
            "extras.note: development-only value (contains '_dev_only')",
        ]

    def test_the_marker_is_found_inside_collections_and_undeclared_keys(self) -> None:
        # A list of models or mappings, and a key `extra="allow"` kept: all settings the scan
        # must read, each reported at the field that holds it.
        class _Upstream(BaseModel):
            token: SecretStr

        class _Loose(BaseModel):
            model_config = ConfigDict(extra="allow")

            upstreams: list[_Upstream] = []
            hooks: list[dict[str, str]] = []

        settings = _Loose(
            upstreams=[_Upstream(token=SecretStr("t_dev_only"))],
            hooks=[{"url": "https://h_dev_only.example"}],
            seeded="s_dev_only",  # type: ignore[call-arg]
        )

        assert sorted(f.target for f in ProductionPosture(settings=settings).findings()) == [
            "hooks",
            "seeded",
            "upstreams",
        ]

    def test_a_rule_over_a_group_reads_every_value_under_it(self) -> None:
        class _Grouped(BaseModel):
            callbacks: dict[str, str] = {}

        posture = ProductionPosture(
            settings=_Grouped(callbacks={"a": "https://a.example", "b": "http://b.example"}),
            rules=(RequireHttps(fields=("callbacks",)),),
        )

        assert [f.render() for f in posture.findings()] == ["callbacks: must be https"]

    @pytest.mark.parametrize("password", ["ab[cd", "ab[cd]ef"])
    def test_a_value_that_is_not_a_url_is_judged_not_raised(self, password: str) -> None:
        # A password nobody URL-encoded makes the DSN unparseable. The posture still answers —
        # it is not https, and it names no loopback host — rather than crashing the boot with
        # a parser error that may quote part of the value.
        class _Raw(BaseModel):
            dsn: str

        posture = ProductionPosture(
            settings=_Raw(dsn=f"postgresql://app:{password}@db.internal/app"),
            rules=(RequireHttps(fields=("dsn",)), LoopbackHost(fields=("dsn",))),
        )

        assert [f.render() for f in posture.findings()] == ["dsn: must be https"]


class TestWhichEnvironmentIsProduction:
    @pytest.mark.parametrize("env", [None, "", "   ", "prod", "production", "Dev"])
    def test_anything_not_declared_non_production_gets_every_rule(self, env: str | None) -> None:
        # Unset, blank, another spelling, a typo — all strict. Only a declared name relaxes it.
        settings = _good(env=env, http=_Http(public_base_url="http://app.example.com"))

        assert _refusals(settings) == ["http.public_base_url: must be https"]

    def test_a_declared_non_production_environment_is_inert(self) -> None:
        settings = _good(env="dev", http=_Http(public_base_url="http://localhost:8000"))

        assert _refusals(settings) == []

    def test_no_environment_path_means_production(self) -> None:
        posture = ProductionPosture(
            settings=_good(env="dev", http=_Http(public_base_url="http://x.example")),
            rules=(RequireHttps(fields=("http.public_base_url",)),),
        )

        assert [f.render() for f in posture.findings()] == ["http.public_base_url: must be https"]


class TestADeclarationThatNamesNothing:
    @pytest.mark.parametrize("env", ["production", "dev"])
    def test_an_unresolvable_path_is_reported_in_every_environment(self, env: str) -> None:
        # A renamed field is a rule nobody enforces; a laptop is where that should surface.
        posture = ProductionPosture(
            settings=_good(env=env),
            environment="env",
            non_production=frozenset({"dev"}),
            rules=(RequireSet(fields=("db.pasword",)),),
        )

        assert [f.render() for f in posture.findings()] == [
            "db.pasword: does not resolve in _Settings"
        ]

    def test_an_unresolvable_path_cannot_be_exempted(self) -> None:
        posture = ProductionPosture(
            settings=_good(),
            rules=(RequireSet(fields=("db.pasword",)),),
            exempt=(Exempt(target="db.pasword", reason="renamed"),),
        )

        assert len(posture.findings()) == 1

    def test_a_path_through_an_unset_parent_reads_as_unset(self) -> None:
        class _Optional(BaseModel):
            db: _Db | None = None

        posture = ProductionPosture(
            settings=_Optional(), rules=(RequireSet(fields=("db.password",)),)
        )

        assert [f.render() for f in posture.findings()] == ["db.password: required and unset"]

    @pytest.mark.parametrize(
        "build",
        [
            lambda: RequireSet(fields="db.password"),  # type: ignore[arg-type]
            lambda: RequireHttps(fields=()),
            lambda: DevValue(pattern=""),
            lambda: Exempt(target="db.dsn", reason="  "),
            lambda: ProductionPosture(settings=_good(), non_production="dev"),  # type: ignore[arg-type]
            lambda: ProductionPosture(settings={"env": "dev"}),  # type: ignore[arg-type]
        ],
        ids=["bare-string-fields", "no-fields", "empty-pattern", "blank-reason", "bare-string-envs", "not-a-model"],
    )
    def test_a_declaration_that_would_check_nothing_is_refused(self, build: Any) -> None:
        with pytest.raises(CoreException) as caught:
            build()

        assert caught.value.kind is ExceptionKind.CONFIGURATION


class TestARefusalNamesKeysNeverValues:
    def test_the_secret_is_nowhere_in_the_refusal_or_the_log(
        self, caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # The DSN carries the secret too, and breaks two rules.
        settings = _good(
            db=_Db(
                dsn=f"postgresql://app:{SECRET}@localhost/app",
                password=SecretStr(f"{SECRET}_dev_only"),
            )
        )

        with pytest.raises(CoreException) as caught:
            build_runtime(posture=_posture(settings))

        error = caught.value
        captured = capsys.readouterr()

        assert error.code == POSTURE_REFUSED
        assert "db.dsn: points at a loopback host" in error.summary
        assert "db.password: development-only value" in error.summary

        for text in (str(error), error.summary, repr(error.details), caplog.text, captured.out, captured.err):
            assert SECRET not in text


class TestBothGates:
    def test_check_wiring_reports_it_and_fails(self) -> None:
        settings = _good(http=_Http(public_base_url="http://app.example.com"))
        report = check_wiring(
            OperationRegistry().freeze(),
            lambda: context_from_modules(),
            posture=_posture(settings),
        )

        assert [f.render() for f in report.findings] == ["http.public_base_url: must be https"]
        assert report.failures == ()
        assert not report.ok

        with pytest.raises(CoreException, match="must be https"):
            report.raise_if_failed()

    def test_the_facade_factory_shortcut_carries_it(self) -> None:
        factory = OperationFacadeFactory(
            OperationFacade, OperationRegistry().freeze(), lambda: context_from_modules()
        )
        settings = _good(http=_Http(public_base_url="http://app.example.com"))

        report = check_facade_factory_wiring(factory, posture=_posture(settings))

        assert [f.render() for f in report.findings] == ["http.public_base_url: must be https"]

    def test_a_clean_posture_leaves_the_report_ok(self) -> None:
        report = check_wiring(
            OperationRegistry().freeze(), lambda: context_from_modules(), posture=_posture(_good())
        )

        assert report.ok

    def test_the_runtime_refuses_without_check_wiring_ever_running(self) -> None:
        settings = _good(db=_Db(dsn="postgresql://app@db.internal/app", password=None))

        with pytest.raises(CoreException) as caught:
            build_runtime(posture=_posture(settings))

        assert caught.value.code == POSTURE_REFUSED
        assert "db.password: required and unset" in caught.value.summary

    def test_a_clean_posture_builds(self) -> None:
        build_runtime(posture=_posture(_good()))


class TestEscalatingAModuleWarning:
    """The cleartext-credentials check warns on its own and refuses under the posture."""

    @staticmethod
    def _module() -> HttpDepsModule:
        return HttpDepsModule(
            client=HttpClient(),
            services={
                "billing": HttpServiceConfig(
                    base_url="http://billing.mesh.svc",
                    auth=HttpAuthConfig(token=SECRET),
                ),
                "secure": HttpServiceConfig(
                    base_url="https://secure.example.com", auth=HttpAuthConfig(token=SECRET)
                ),
            },
        )

    def test_without_a_posture_it_only_warns(self) -> None:
        build_runtime(self._module())

    def test_a_non_production_posture_leaves_it_a_warning(self) -> None:
        build_runtime(self._module(), posture=_posture(_good(env="dev")))

    def test_a_production_posture_refuses_it(self) -> None:
        with pytest.raises(CoreException) as caught:
            build_runtime(self._module(), posture=_posture(_good()))

        assert "http_service:billing: sends a credential" in caught.value.summary
        assert "http_service:secure" not in caught.value.summary
        assert SECRET not in caught.value.summary

    def test_check_wiring_reports_it_too(self) -> None:
        report = check_wiring(
            OperationRegistry().freeze(),
            lambda: context_from_modules(self._module()),
            posture=_posture(_good()),
        )

        assert [f.target for f in report.findings] == ["http_service:billing"]

    def test_an_exemption_with_its_reason_lets_it_through(self) -> None:
        mesh = Exempt(target="http_service:billing", reason="the mesh terminates TLS in a sidecar")

        build_runtime(self._module(), posture=_posture(_good(), exempt=(mesh,)))
