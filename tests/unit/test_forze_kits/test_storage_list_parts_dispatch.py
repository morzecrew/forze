"""LIST_PARTS is a COMMAND and dispatches through ``run_operation`` (route path).

LIST_PARTS reads a session's part listing but acquires the write-guarded
``uploads`` port, so it must be classified as a command rather than a query. It
is exercised here the way the FastAPI route does — through ``run_operation`` —
and its plan kind is pinned to ``COMMAND`` (a QUERY classification would be a
latent contradiction with the write-guarded port it acquires).
"""

from __future__ import annotations

import pytest

from forze.application.contracts.storage import StorageSpec
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.planning.plans import OperationKind
from forze_kits.aggregates.storage import (
    StorageKernelOp,
    build_storage_registry,
)
from forze_kits.aggregates.storage.dto import (
    BeginUploadRequestDTO,
    UploadSessionRequestDTO,
)
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules

pytestmark = pytest.mark.unit

_FILES = StorageSpec(name="files")


def test_list_parts_is_bound_as_command_not_query() -> None:
    # The QUERY group holds only true read ops; LIST_PARTS must not be among them
    # (it acquires the write-guarded uploads port, so it is a COMMAND).
    reg = build_storage_registry(_FILES)
    ns = _FILES.default_namespace

    query_plans = {
        op
        for op, plan in reg.get_plans().items()
        if plan.kind is OperationKind.QUERY
    }

    assert ns.key(StorageKernelOp.LIST_PARTS) not in query_plans
    assert ns.key(StorageKernelOp.LIST) in query_plans  # control: a real query


async def test_list_parts_dispatches_via_run_operation() -> None:
    # Shared state so the session opened by BEGIN_UPLOAD is visible to LIST_PARTS.
    state = MockState()
    ns = _FILES.default_namespace
    reg = build_storage_registry(_FILES).freeze()

    begin_ctx = context_from_modules(MockDepsModule(state=state))
    list_ctx = context_from_modules(MockDepsModule(state=state))

    session = await run_operation(
        reg,
        ns.key(StorageKernelOp.BEGIN_UPLOAD),
        BeginUploadRequestDTO(key="big.bin"),
        begin_ctx,
    )

    res = await run_operation(
        reg,
        ns.key(StorageKernelOp.LIST_PARTS),
        UploadSessionRequestDTO(session=session),
        list_ctx,
    )

    assert res.parts == []
