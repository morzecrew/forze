"""Coverage for the tracing port proxy's value-capture and async-generator paths."""

from __future__ import annotations

from typing import Any, AsyncIterator

import attrs
import pytest

from ipaddress import IPv4Address

from forze.application.contracts.base.value_objects import CountlessPage
from forze.application.execution import DepsRegistry
from forze.application.execution.tracing.port_proxy import TracingPortProxy, wrap_port
from forze.domain.models import Document
from forze_mock import MockDepsModule, MockState

# ----------------------- #


def _tracing_deps() -> Any:
    return (
        DepsRegistry.from_modules(lambda: MockDepsModule(state=MockState())())
        .with_tracing(runtime=True)
        .freeze()
        .resolve()
    )


class _Cell(Document):
    value: int


class _Device(Document):
    ip: IPv4Address


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
# Phase-2 capture for the predicate (phantom) oracle: the scan filter on a query call event, and a
# scan page's hits unwrapped into per-row return events.


class TestQueryPredicateCapture:
    def _query_proxy(self) -> TracingPortProxy:
        return TracingPortProxy(
            inner=object(),
            deps=_tracing_deps(),
            domain="document",
            surface="document_query",
            route="cells",
            phase="query",
            capture=True,
        )

    def test_query_capture_is_the_leading_filter(self) -> None:
        # count(filter) / find_many(filter): the filter is the leading positional → the captured input.
        proxy = self._query_proxy()
        assert proxy._captured_in(({"$values": {"value": 7}},), {}) == {
            "$values": {"value": 7}
        }

    def test_query_capture_ignores_a_trailing_pagination(self) -> None:
        # find_many(None, pagination): a match-all filter must NOT pull the pagination dict instead.
        proxy = self._query_proxy()
        assert proxy._captured_in((None, {"limit": 10, "offset": 0}), {}) is None

    def test_query_capture_reads_a_filters_kwarg(self) -> None:
        proxy = self._query_proxy()
        assert proxy._captured_in((), {"filters": {"$values": {"value": 9}}}) == {
            "$values": {"value": 9}
        }

    def test_command_capture_is_unchanged_first_structured_arg(self) -> None:
        # A command keeps the original rule: the first structured argument is the write payload.
        proxy = TracingPortProxy(
            inner=object(),
            deps=_tracing_deps(),
            domain="document",
            surface="document_command",
            route="cells",
            phase="command",
            capture=True,
        )
        assert proxy._captured_in(({"value": 5},), {}) == {"value": 5}

    def test_record_return_unwraps_page_hits_into_per_row_events(self) -> None:
        # find_many returns a CountlessPage; attrs.asdict would leave the nested pydantic hits
        # un-dumped, so each hit is recorded as its own return event carrying id + rev + fields.
        deps = _tracing_deps()
        proxy = TracingPortProxy(
            inner=object(),
            deps=deps,
            domain="document",
            surface="document_query",
            route="cells",
            phase="query",
            capture=True,
        )
        page = CountlessPage(hits=[_Cell(value=1), _Cell(value=2)], page=1, size=2)

        proxy._record_return("find_many", ({"$values": {"value": 1}},), page)

        trace = deps.runtime_trace()
        assert trace is not None
        hits = [event.result for event in trace.events if event.result is not None]
        assert len(hits) == 2
        assert {hit["value"] for hit in hits} == {1, 2}
        assert all("id" in hit and "rev" in hit for hit in hits)

    def test_command_result_keeps_a_native_typed_copy_for_the_oracle(self) -> None:
        # A write result carries BOTH the JSON ``result`` (portable: IP → str) and the native
        # ``result_native`` (IPv4Address kept) — the isolation oracle matches predicates against the
        # native form so its match agrees with the backend's in-memory scan rather than a JSON string.
        deps = _tracing_deps()
        proxy = TracingPortProxy(
            inner=object(),
            deps=deps,
            domain="document",
            surface="document_command",
            route="devices",
            phase="command",
            capture=True,
        )

        proxy._record_return("create", (), _Device(ip=IPv4Address("10.0.0.1")))

        event = next(e for e in deps.runtime_trace().events if e.result is not None)
        assert event.result["ip"] == "10.0.0.1" and isinstance(event.result["ip"], str)
        assert event.result_native is not None
        assert event.result_native["ip"] == IPv4Address("10.0.0.1")

    def test_create_result_backfills_the_write_key_for_the_oracle(self) -> None:
        # A create has no leading id in its args, so the call key is ``None`` — but the result carries
        # the assigned id; the return event backfills it (id-only) so the pairwise isolation oracle,
        # which attributes writes by key, sees create/batch writes too, not only single-key updates.
        deps = _tracing_deps()
        proxy = TracingPortProxy(
            inner=object(),
            deps=deps,
            domain="document",
            surface="document_command",
            route="cells",
            phase="command",
            capture=True,
        )
        cell = _Cell(value=7)

        proxy._record_return("create", (), cell)  # no leading-id arg, like a real create

        event = next(e for e in deps.runtime_trace().events if e.result is not None)
        assert event.key == str(cell.id)

    def test_query_result_has_no_native_copy(self) -> None:
        # Reads keep only the JSON ``result`` (the native copy is a write-only concern for the oracle).
        deps = _tracing_deps()
        proxy = TracingPortProxy(
            inner=object(),
            deps=deps,
            domain="document",
            surface="document_query",
            route="devices",
            phase="query",
            capture=True,
        )

        proxy._record_return("get", (), _Device(ip=IPv4Address("10.0.0.1")))

        event = next(e for e in deps.runtime_trace().events if e.result is not None)
        assert event.result_native is None


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
