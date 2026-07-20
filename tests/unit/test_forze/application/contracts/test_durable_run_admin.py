"""Durable-run admin plane: opaque cursor codec and page trimming.

# covers: encode_run_cursor
# covers: decode_run_cursor
# covers: build_run_page
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from forze.application.contracts.durable.function import (
    DurableRunRecord,
    DurableRunStatus,
    build_run_page,
    decode_run_cursor,
    encode_run_cursor,
)
from forze.base.codecs import B64UrlJsonCodec
from forze.base.exceptions import CoreException

# ----------------------- #

UTC = UTC
_CURSOR_CODEC = B64UrlJsonCodec()  # same encoding decode_run_cursor consumes


def _record(run_id: str, created_at: datetime) -> DurableRunRecord:
    return DurableRunRecord(
        run_id=run_id,
        name="fn",
        status=DurableRunStatus.PENDING,
        created_at=created_at,
    )


class TestRunCursorCodec:
    def test_round_trips_created_at_and_run_id(self) -> None:
        ts = datetime(2026, 7, 7, 12, 0, 0, 123456, tzinfo=UTC)

        decoded_ts, decoded_id = decode_run_cursor(encode_run_cursor(ts, "run-1"))

        assert decoded_ts == ts
        assert decoded_id == "run-1"

    def test_cursor_is_opaque(self) -> None:
        token = encode_run_cursor(datetime(2026, 1, 1, tzinfo=UTC), "run-1")

        # An opaque token does not leak the raw values verbatim.
        assert "run-1" not in token
        assert ":" not in token

    @pytest.mark.parametrize("bad", ["", "not-base64!", "YWJj"])
    def test_rejects_malformed_cursor(self, bad: str) -> None:
        with pytest.raises(CoreException):
            decode_run_cursor(bad)

    def test_rejects_naive_timestamp_cursor(self) -> None:
        # A well-formed token whose ts is timezone-naive can only be hand-crafted; accepting
        # it would let Postgres reinterpret the boundary in the server timezone.
        naive = _CURSOR_CODEC.dumps({"ts": "2026-07-07T12:00:00", "id": "run-1"})

        with pytest.raises(CoreException):
            decode_run_cursor(naive)


class TestBuildRunPage:
    def test_trims_overfetch_and_seeds_next_cursor(self) -> None:
        ts = datetime(2026, 7, 7, tzinfo=UTC)
        # Three records for a limit of 2 → one extra signals a further page.
        records = [_record(f"r{i}", ts) for i in range(3)]

        page = build_run_page(records, limit=2)

        assert [r.run_id for r in page.records] == ["r0", "r1"]
        assert page.next_cursor is not None
        # The cursor points at the last kept record, so the next page seeks past it.
        assert decode_run_cursor(page.next_cursor) == (ts, "r1")

    def test_no_next_cursor_when_page_not_full(self) -> None:
        ts = datetime(2026, 7, 7, tzinfo=UTC)
        records = [_record("r0", ts), _record("r1", ts)]

        page = build_run_page(records, limit=2)

        assert [r.run_id for r in page.records] == ["r0", "r1"]
        assert page.next_cursor is None

    def test_raises_when_boundary_record_has_no_created_at(self) -> None:
        ts = datetime(2026, 7, 7, tzinfo=UTC)
        # A further page exists but the boundary record carries no timestamp: silently ending
        # would hide older runs, so building the page must fail loud.
        records = [
            DurableRunRecord(
                run_id="r0", name="fn", status=DurableRunStatus.PENDING, created_at=None
            ),
            _record("r1", ts),
        ]

        with pytest.raises(CoreException):
            build_run_page(records, limit=1)
