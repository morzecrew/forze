"""The container adapter against the shared battery — the real leg.

The container tier is where the plane's headline gate finally has a passing side, so this
leg proves the branch the mock's cannot: untrusted provenance *resolves* here, because the
isolation can hold it. Everything else in the battery is the seam both engines owe.
"""

from __future__ import annotations

from typing import cast

import pytest

from forze.application.contracts.sandbox import (
    ProgramPayload,
    Provenance,
    SandboxPort,
    SandboxRequest,
    SandboxSpec,
)
from forze.application.execution import ExecutionContext
from forze_sandbox_container import ConfigurableContainerSandbox, container_capabilities
from tests.integration.test_forze_sandbox_container.conftest import container_config
from tests.support.sandbox_conformance import (
    SANDBOX_BATTERY,
    Check,
    SandboxHarness,
    SandboxScript,
)

# ----------------------- #

pytestmark = pytest.mark.integration

_ROUTE = "jobs"
_UNCAPPED = "jobs_uncapped"


@pytest.fixture
def harness(ctx: ExecutionContext) -> SandboxHarness:
    capped = container_config(memory_ceiling=64 * 1024 * 1024)

    def invoke(script: SandboxScript) -> tuple[SandboxPort, SandboxRequest]:
        port = ConfigurableContainerSandbox(config=capped)(
            ctx, SandboxSpec(name=_ROUTE, provenance="untrusted")
        )
        source = (
            "import sys\n"
            f"sys.stdout.write({script.stdout!r})\n"
            f"sys.stderr.write({script.stderr!r})\n"
            f"sys.exit({script.exit_code})\n"
        )

        return port, SandboxRequest(
            program=ProgramPayload(interpreter=("python", "-u"), source=source)
        )

    def resolve(provenance: str) -> SandboxPort:
        return ConfigurableContainerSandbox(config=capped)(
            ctx, SandboxSpec(name=_ROUTE, provenance=cast("Provenance", provenance))
        )

    def uncapped() -> SandboxPort:
        return ConfigurableContainerSandbox(config=container_config())(
            ctx, SandboxSpec(name=_UNCAPPED, provenance="untrusted")
        )

    return SandboxHarness(
        backend="container",
        capabilities=container_capabilities(capped),
        invoke=invoke,
        resolve=resolve,
        uncapped=uncapped,
    )


@pytest.mark.conformance(plane="sandbox", engine="sandbox_container")
@pytest.mark.parametrize("check", SANDBOX_BATTERY, ids=lambda check: check.__name__)
async def test_sandbox_battery(check: Check, harness: SandboxHarness) -> None:
    await check(harness)
