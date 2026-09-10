"""The base subprocess adapter against the shared battery — the real leg that needs no daemon.

Three engines answer this port, and this is the middle one: it really starts a process and
really kills it, and it cannot hold unreviewed code. So it proves the same refusal branch
the mock's route stands in for, from an adapter that is not standing in for anything.
"""

from __future__ import annotations

import sys
from datetime import timedelta
from typing import Any, cast

import pytest

from forze.application.contracts.sandbox import (
    ProgramPayload,
    Provenance,
    SandboxPort,
    SandboxRequest,
    SandboxSpec,
)
from forze.application.contracts.storage import StorageSpec
from forze.application.execution import ExecutionContext
from forze.application.integrations.sandbox import (
    ConfigurableSubprocessSandbox,
    SubprocessSandboxConfig,
    subprocess_capabilities,
)
from forze.testing import context_from_modules
from forze_mock import MockDepsModule, MockState
from tests.support.sandbox_conformance import (
    SANDBOX_BATTERY,
    Check,
    SandboxHarness,
    SandboxScript,
)

# ----------------------- #

pytestmark = pytest.mark.unit

_BLOBS = StorageSpec(name="sandbox_files")
_ROUTE = "jobs"
_UNCAPPED = "jobs_uncapped"


def _config(**overrides: Any) -> SubprocessSandboxConfig:
    settings: dict[str, Any] = {
        "provenance": "trusted",
        "wall_clock_ceiling": timedelta(seconds=20),
        "max_output_bytes": 64 * 1024,
        "acknowledge_network_egress": True,
        "storage": _BLOBS,
        "kill_grace": timedelta(milliseconds=200),
    }
    settings.update(overrides)

    return SubprocessSandboxConfig(**settings)


@pytest.fixture
def harness() -> SandboxHarness:
    ctx: ExecutionContext = context_from_modules(MockDepsModule(state=MockState()))
    capped = _config(memory_ceiling=512 * 1024 * 1024)

    def invoke(script: SandboxScript) -> tuple[SandboxPort, SandboxRequest]:
        port = ConfigurableSubprocessSandbox(config=capped)(
            ctx, SandboxSpec(name=_ROUTE, provenance="trusted")
        )
        source = (
            "import sys\n"
            f"sys.stdout.write({script.stdout!r})\n"
            f"sys.stderr.write({script.stderr!r})\n"
            f"sys.exit({script.exit_code})\n"
        )

        return port, SandboxRequest(
            program=ProgramPayload(interpreter=(sys.executable, "-u"), source=source)
        )

    def resolve(provenance: str) -> SandboxPort:
        return ConfigurableSubprocessSandbox(config=capped)(
            ctx, SandboxSpec(name=_ROUTE, provenance=cast("Provenance", provenance))
        )

    def uncapped() -> SandboxPort:
        return ConfigurableSubprocessSandbox(config=_config())(
            ctx, SandboxSpec(name=_UNCAPPED, provenance="trusted")
        )

    return SandboxHarness(
        backend="subprocess",
        capabilities=subprocess_capabilities(capped),
        invoke=invoke,
        resolve=resolve,
        uncapped=uncapped,
    )


@pytest.mark.conformance(plane="sandbox", engine="subprocess")
@pytest.mark.parametrize("check", SANDBOX_BATTERY, ids=lambda check: check.__name__)
async def test_sandbox_battery(check: Check, harness: SandboxHarness) -> None:
    await check(harness)
