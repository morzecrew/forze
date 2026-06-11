"""Tests for HttpDepsModule wiring."""


from forze.application.contracts.http import HttpServiceDepKey
from forze_http.execution.deps.configs import HttpServiceConfig
from forze_http.execution.deps.keys import HttpClientDepKey
from forze_http.execution.deps.module import HttpDepsModule
from forze_http.kernel.client import HttpClient

# ----------------------- #


def test_deps_module_registers_routes() -> None:
    client = HttpClient()
    module = HttpDepsModule(
        client=client,
        services={
            "demo": HttpServiceConfig(base_url="https://example.com"),
        },
    )

    deps = module()
    routed = deps.routed_deps or {}

    assert HttpServiceDepKey in routed
    assert "demo" in routed[HttpServiceDepKey]
    plain = deps.plain_deps or {}
    assert HttpClientDepKey in plain
