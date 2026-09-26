"""The in-memory document store against the owned-reads battery.

# covers: DocumentQueryPort.get
# covers: DocumentQueryPort.get_many
"""

from __future__ import annotations

import pytest

from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules
from tests.support.owned_reads_conformance import (
    OWNED_READS_BATTERY,
    Check,
    OwnedReadsHarness,
    owned_spec,
)

# ----------------------- #


@pytest.fixture
def harness() -> OwnedReadsHarness:
    spec = owned_spec("owned_notes")
    ctx = context_from_modules(MockDepsModule())

    return OwnedReadsHarness(
        query=ctx.doc.query(spec),
        command=ctx.doc.command(spec),
        spec_name="owned_notes",
    )


@pytest.mark.conformance(plane="owned_reads", engine="mock")
@pytest.mark.parametrize("check", OWNED_READS_BATTERY, ids=lambda check: check.__name__)
async def test_owned_reads_battery(check: Check, harness: OwnedReadsHarness) -> None:
    await check(harness)
