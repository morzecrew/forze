"""`FrozenOperationRegistry.fingerprint` — a stable structural version of the catalog.

It changes when an operation's observable contract or declared plan facts change
(input/output schema, kind, idempotency/authn/permission/deadline) and stays stable
otherwise. It is deliberately structural, not behavioral — handler code is not hashed.
"""

from __future__ import annotations

from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry

# ----------------------- #


class _In(BaseModel):
    value: int


class _Wide(BaseModel):
    value: int
    note: str


class _Echo(Handler[_In, _In]):
    async def __call__(self, args: _In) -> _In:
        return args


def _registry(*, input_type: type[BaseModel] = _In, query: bool = False) -> object:
    reg = OperationRegistry(
        handlers={"do": lambda _c: _Echo()},
        descriptors={
            "do": OperationDescriptor(input_type=input_type, output_type=_In, description="d")
        },
    )
    if query:
        reg = reg.bind("do").as_query().finish()
    return reg.freeze()


class TestOperationFingerprint:
    def test_stable_and_deterministic(self) -> None:
        assert _registry().fingerprint() == _registry().fingerprint()

    def test_changes_with_input_contract(self) -> None:
        assert _registry(input_type=_In).fingerprint() != _registry(input_type=_Wide).fingerprint()

    def test_changes_with_operation_kind(self) -> None:
        assert _registry(query=False).fingerprint() != _registry(query=True).fingerprint()

    def test_per_operation_fingerprint(self) -> None:
        registry = _registry()
        assert registry.operation_fingerprint("do")
        # the per-op fingerprint of the only op differs from the whole-catalog hash
        # (the catalog hash keys by op name), but is itself stable.
        assert registry.operation_fingerprint("do") == _registry().operation_fingerprint("do")
