"""Coverage tests for :class:`MockAnalyticsAdapter`."""

from __future__ import annotations

import attrs

import pytest

from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
)
from forze.base.exceptions import CoreException
from forze_mock.adapters.analytics import MockAnalyticsAdapter
from forze_mock.state import MockState

# ----------------------- #


class _Row(BaseModel):
    value: int


class _Params(BaseModel):
    day: str = "2026-01-01"


class _OtherParams(BaseModel):
    """A different model with a compatible field for the codec path."""

    day: str = "2026-01-01"


class _Ingest(BaseModel):
    event: str


def _spec(*, ingest: bool = True) -> AnalyticsSpec[_Row, _Ingest]:
    return AnalyticsSpec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
        ingest=_Ingest if ingest else None,
    )


def _adapter(
    state: MockState,
    spec: AnalyticsSpec[_Row, _Ingest] | None = None,
) -> MockAnalyticsAdapter[_Row, _Ingest]:
    return MockAnalyticsAdapter(state=state, spec=spec or _spec())


def _seed(state: MockState) -> None:
    state.analytics_query_hits["events"] = {
        "counts": [{"value": 10}, {"value": 20}, {"value": 30}],
    }


# ----------------------- #


async def test_validated_params_codec_path() -> None:
    """Passing a different-but-structurally-compatible BaseModel hits the codec."""
    state = MockState()
    _seed(state)
    port = _adapter(state)
    # _OtherParams is not an instance of _Params -> decode_mapping path.
    page = await port.run_page("counts", _OtherParams())
    assert page.count == 3


async def test_validated_params_unknown_query_key() -> None:
    state = MockState()
    port = _adapter(state)
    with pytest.raises(CoreException):
        await port.run("missing", _Params())


async def test_validated_params_non_basemodel_raises() -> None:
    """A non-Pydantic params value hits the final defensive raise."""
    state = MockState()
    _seed(state)
    port = _adapter(state)
    with pytest.raises(CoreException):
        await port.run("counts", object())  # type: ignore[arg-type]


async def test_offset_dry_run_with_count() -> None:
    state = MockState()
    _seed(state)
    port = _adapter(state)
    page = await port.run_page("counts", _Params(), options={"dry_run": True})
    assert page.count == 0
    assert list(page.hits) == []


async def test_offset_dry_run_without_count() -> None:
    state = MockState()
    _seed(state)
    port = _adapter(state)
    page = await port.run("counts", _Params(), options={"dry_run": True})
    assert list(page.hits) == []


async def test_cursor_dry_run() -> None:
    state = MockState()
    _seed(state)
    port = _adapter(state)
    page = await port.run_cursor("counts", _Params(), options={"dry_run": True})
    assert list(page.hits) == []
    assert page.has_more is False
    assert page.next_cursor is None


async def test_cursor_page_returns_hits() -> None:
    state = MockState()
    _seed(state)
    port = _adapter(state)
    page = await port.run_cursor("counts", _Params())
    assert [h.value for h in page.hits] == [10, 20, 30]


async def test_run_chunked_yields_batches() -> None:
    state = MockState()
    _seed(state)
    port = _adapter(state)
    chunks = [list(c) async for c in port.run_chunked("counts", _Params(), fetch_batch_size=2)]
    assert [v.value for c in chunks for v in c] == [10, 20, 30]
    assert len(chunks) == 2


async def test_run_chunked_dry_run_yields_nothing() -> None:
    state = MockState()
    _seed(state)
    port = _adapter(state)
    chunks = [
        c
        async for c in port.run_chunked(
            "counts", _Params(), options={"dry_run": True}, fetch_batch_size=2
        )
    ]
    assert chunks == []


async def test_project_run_chunked() -> None:
    state = MockState()
    _seed(state)
    port = _adapter(state)
    chunks = [
        list(c)
        async for c in port.project_run_chunked(
            ["value"], "counts", _Params(), fetch_batch_size=10
        )
    ]
    assert len(chunks) == 1
    assert [r["value"] for r in chunks[0]] == [10, 20, 30]


async def test_select_run_chunked() -> None:
    state = MockState()
    _seed(state)
    port = _adapter(state)
    chunks = [
        list(c)
        async for c in port.select_run_chunked(
            _Row, "counts", _Params(), fetch_batch_size=10
        )
    ]
    assert len(chunks) == 1
    assert [r.value for r in chunks[0]] == [10, 20, 30]


async def test_project_and_select_pages_and_cursors() -> None:
    state = MockState()
    _seed(state)
    port = _adapter(state)

    proj = await port.project_run(["value"], "counts", _Params())
    assert [r["value"] for r in proj.hits] == [10, 20, 30]

    proj_page = await port.project_run_page(["value"], "counts", _Params())
    assert proj_page.count == 3

    proj_cur = await port.project_run_cursor(["value"], "counts", _Params())
    assert [r["value"] for r in proj_cur.hits] == [10, 20, 30]

    sel = await port.select_run(_Row, "counts", _Params())
    assert [r.value for r in sel.hits] == [10, 20, 30]

    sel_page = await port.select_run_page(_Row, "counts", _Params())
    assert sel_page.count == 3

    sel_cur = await port.select_run_cursor(_Row, "counts", _Params())
    assert [r.value for r in sel_cur.hits] == [10, 20, 30]


async def test_max_rows_option() -> None:
    state = MockState()
    _seed(state)
    port = _adapter(state)
    page = await port.run_page("counts", _Params(), options={"max_rows": 2})
    assert page.count == 2


async def test_append_ingest_not_configured() -> None:
    state = MockState()
    port = _adapter(state, _spec(ingest=False))
    with pytest.raises(CoreException):
        await port.append([_Ingest(event="x")])


async def test_append_empty_rows() -> None:
    state = MockState()
    port = _adapter(state)
    result = await port.append([])
    assert result is not None
    assert result.accepted == 0


async def test_append_ingest_codec_none() -> None:
    """ingest set but resolved codec is None -> internal error branch."""

    class _SpecNoCodec(AnalyticsSpec[_Row, _Ingest]):
        @property
        def resolved_ingest_codec(self):  # type: ignore[override]
            return None

    spec = _SpecNoCodec(
        name="events",
        read=_Row,
        queries={"counts": AnalyticsQueryDefinition(params=_Params)},
        ingest=_Ingest,
    )
    state = MockState()
    port = MockAnalyticsAdapter(state=state, spec=spec)
    with pytest.raises(CoreException):
        await port.append([_Ingest(event="x")])


async def test_append_success() -> None:
    state = MockState()
    port = _adapter(state)
    result = await port.append([_Ingest(event="a"), _Ingest(event="b")])
    assert result is not None
    assert result.accepted == 2
    assert len(state.analytics_ingest_log["events"]) == 2
