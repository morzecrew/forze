"""Tests for HttpxDepsModule wiring."""


from forze.application.contracts.http import HttpServiceDepKey
from forze_http.execution.deps.configs import HttpxHttpServiceConfig
from forze_http.execution.deps.keys import HttpxClientDepKey
from forze_http.execution.deps.module import HttpxDepsModule
from forze_http.kernel.client import HttpxClient

# ----------------------- #


def test_deps_module_registers_routes() -> None:
    client = HttpxClient()
    module = HttpxDepsModule(
        client=client,
        services={
            "demo": HttpxHttpServiceConfig(base_url="https://example.com"),
        },
    )

    deps = module()
    routed = deps.routed_deps or {}

    assert HttpServiceDepKey in routed
    assert "demo" in routed[HttpServiceDepKey]
    plain = deps.plain_deps or {}
    assert HttpxClientDepKey in plain
