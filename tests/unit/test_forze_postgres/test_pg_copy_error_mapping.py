"""The ``COPY`` error mapper's deferral rule, which only runs when it must *not* act.

`copy_data_error` claims a narrow slice of failures: those the server attributed to a
specific input line. Everything else — a timeout, a serialization failure, a driver-side
adaptation error — keeps the mapping it has always had. That deferral is a detection branch
in reverse: it fires precisely when the error is *not* the stream's fault, and if it were
dead every such failure would be relabelled `copy_row_invalid` and a caller would go looking
for a bad row that does not exist.

Unit-level because the branch needs an error carrying no COPY context, which is what
psycopg raises client-side, before the server is involved.
"""

from __future__ import annotations

import pytest

pytest.importorskip("psycopg")

from psycopg import errors

from forze_postgres.kernel.client.errors import copy_data_error

# ----------------------- #


class TestDeferral:
    """What the mapper declines to claim."""

    @pytest.mark.parametrize(
        "error",
        [
            errors.DataError("adaptation failed before the server saw anything"),
            errors.ProtocolViolation("connection-level protocol failure"),
            errors.BadCopyFileFormat("malformed, but not attributed to a line"),
        ],
    )
    @pytest.mark.parametrize("binary", [False, True])
    def test_an_error_without_copy_context_is_left_alone(
        self,
        error: Exception,
        binary: bool,
    ) -> None:
        """No ``COPY … line N`` means the server did not blame the stream."""

        assert copy_data_error(error, binary=binary) is None

    @pytest.mark.parametrize(
        "error",
        [
            errors.QueryCanceled("statement timeout"),
            errors.SerializationFailure("could not serialize access"),
            errors.UndefinedTable("relation does not exist"),
            errors.InsufficientPrivilege("permission denied"),
        ],
    )
    def test_failures_outside_the_copy_families_are_left_alone(self, error: Exception) -> None:
        """A copy can fail for reasons that have nothing to do with its rows.

        These already map to kinds callers act on — a timeout is retryable, a missing
        relation is a wiring error — and relabelling them as a bad row would both mislead
        and change their retry disposition.
        """

        assert copy_data_error(error, binary=False) is None

    def test_a_non_psycopg_error_is_left_alone(self) -> None:
        assert copy_data_error(ValueError("not a database error at all"), binary=True) is None
