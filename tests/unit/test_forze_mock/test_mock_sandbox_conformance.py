"""The in-memory sandbox against the shared battery — the oracle's leg.

The mock is what DST and every unit test resolve, so its answers are the ones a simulation
believes. This leg is what makes "passed against the mock" mean "matches the adapter a
deployment actually runs".

Its route stands in for the **base subprocess tier** rather than for the mock's own default
surface. That is the wiring an application simulating its production shape would write, and
it is the branch of the provenance rule the container leg cannot prove: a route whose
isolation cannot hold unreviewed code has to refuse it, and only an engine below the
container tier can demonstrate the refusal.
"""

from __future__ import annotations

from typing import cast

import pytest

from forze.application.contracts.sandbox import (
    CapturedStream,
    Provenance,
    SandboxPort,
    SandboxRequest,
    SandboxResult,
    SandboxSpec,
)
from forze.application.integrations.sandbox import SUBPROCESS_CAPABILITIES
from forze.testing import context_from_modules
from forze_mock import MockDepsModule, MockSandboxRegistry, MockState
from forze_mock.execution.factories import ConfigurableMockSandbox
from tests.support.sandbox_conformance import (
    SANDBOX_BATTERY,
    Check,
    SandboxHarness,
    SandboxScript,
)

# ----------------------- #

pytestmark = pytest.mark.unit

_ROUTE = "jobs"
_UNCAPPED = "jobs_uncapped"


@pytest.fixture
def harness() -> SandboxHarness:
    registry = MockSandboxRegistry()
    module = MockDepsModule(state=MockState(), sandboxes=registry)
    ctx = context_from_modules(module)
    factory = ConfigurableMockSandbox(module=module)

    def invoke(script: SandboxScript) -> tuple[SandboxPort, SandboxRequest]:
        registry.on(
            _ROUTE,
            lambda _: SandboxResult(
                outcome="exited",
                exit_code=script.exit_code,
                stdout=CapturedStream(text=script.stdout, byte_count=len(script.stdout.encode())),
                stderr=CapturedStream(text=script.stderr, byte_count=len(script.stderr.encode())),
            ),
            capabilities=SUBPROCESS_CAPABILITIES,
        )

        return factory(ctx, SandboxSpec(name=_ROUTE, provenance="trusted")), SandboxRequest(
            command=("true",)
        )

    def resolve(provenance: str) -> SandboxPort:
        registry.on(_ROUTE, _unreached, capabilities=SUBPROCESS_CAPABILITIES)

        return factory(ctx, SandboxSpec(name=_ROUTE, provenance=cast("Provenance", provenance)))

    def uncapped() -> SandboxPort:
        registry.on(_UNCAPPED, _unreached, capabilities=SUBPROCESS_CAPABILITIES)

        return factory(ctx, SandboxSpec(name=_UNCAPPED, provenance="trusted"))

    return SandboxHarness(
        backend="mock",
        capabilities=SUBPROCESS_CAPABILITIES,
        invoke=invoke,
        resolve=resolve,
        uncapped=uncapped,
    )


def _unreached(request: SandboxRequest) -> SandboxResult:
    """Registered where the check refuses before the run — reaching it is the failure."""

    raise AssertionError(f"the route ran {request.argv} where the gate should have refused")


@pytest.mark.conformance(plane="sandbox", engine="mock")
@pytest.mark.parametrize("check", SANDBOX_BATTERY, ids=lambda check: check.__name__)
async def test_sandbox_battery(check: Check, harness: SandboxHarness) -> None:
    await check(harness)
