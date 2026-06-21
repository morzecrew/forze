"""Coverage for the tracing port proxy's value-capture and async-generator paths."""

from __future__ import annotations

from typing import Any, AsyncIterator

import attrs
import pytest

from forze.application.execution import DepsRegistry
from forze.application.execution.tracing.port_proxy import TracingPortProxy, wrap_port
from forze_mock import MockDepsModule, MockState

# ----------------------- #


def _tracing_deps() -> Any:
    return (
        DepsRegistry.from_modules(lambda: MockDepsModule(state=MockState())())
        .with_tracing(runtime=True)
        .freeze()
        .resolve()
    )


@attrs.define
class _AttrsValue:
    a: int


class _ModelWithMode:
    def model_dump(self, mode: str | None = None) -> dict[str, int]:
        return {"a": 1}


class _ModelNoMode:
    """``model_dump`` without a ``mode`` kwarg — forces the ``TypeError`` fallback."""

    def model_dump(self) -> dict[str, int]:
        return {"a": 1}


class _ModelNonDict:
    def model_dump(self, mode: str | None = None) -> list[int]:
        return [1, 2]


# ....................... #


class TestDump:
    def test_scalar_returns_none(self) -> None:
        assert TracingPortProxy._dump(5) is None
        assert TracingPortProxy._dump(None) is None

    def test_model_dump_with_mode(self) -> None:
        assert TracingPortProxy._dump(_ModelWithMode()) == {"a": 1}

    def test_model_dump_without_mode_falls_back(self) -> None:
        assert TracingPortProxy._dump(_ModelNoMode()) == {"a": 1}

    def test_model_dump_non_dict_returns_none(self) -> None:
        assert TracingPortProxy._dump(_ModelNonDict()) is None

    def test_attrs_value(self) -> None:
        assert TracingPortProxy._dump(_AttrsValue(a=2)) == {"a": 2}

    def test_mapping(self) -> None:
        assert TracingPortProxy._dump({"k": 1}) == {"k": 1}

    def test_unstructured_returns_none(self) -> None:
        assert TracingPortProxy._dump(object()) is None


# ....................... #


class TestPayloadAndReturn:
    def _proxy(self, *, capture: bool) -> TracingPortProxy:
        return TracingPortProxy(
            inner=object(),
            deps=_tracing_deps(),
            domain="document",
            surface="document_command",
            route="orders",
            phase="command",
            capture=capture,
        )

    def test_payload_of_none_when_no_structured_arg(self) -> None:
        proxy = self._proxy(capture=True)

        assert proxy._payload_of((5, "scalar"), {}) is None

    def test_record_return_noop_for_scalar_result(self) -> None:
        proxy = self._proxy(capture=True)

        # A scalar result dumps to ``None`` → the record is skipped (no raise, no event).
        proxy._record_return("get", (), 42)


# ....................... #


class TestAsyncGenerator:
    @pytest.mark.asyncio
    async def test_async_gen_is_traced(self) -> None:
        deps = _tracing_deps()

        class _Inner:
            async def stream(self) -> AsyncIterator[int]:
                yield 1
                yield 2

        wrapped = wrap_port(
            _Inner(),
            deps=deps,
            domain="document",
            surface="document_query",
            route=None,
            phase="query",
        )

        items = [item async for item in wrapped.stream()]

        assert items == [1, 2]

        trace = deps.runtime_trace()
        assert trace is not None
        assert any(event.op == "stream" for event in trace.events)
