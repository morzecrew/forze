"""Shared conformance battery for ``rollback_only()`` on a transaction scope.

A scope marked ``rollback_only()`` rolls back instead of committing, and the caller carries on as
after a commit (a dry run). The battery checks what every backend must agree on: the writes are
gone, nothing logs an error, after-commit callbacks never run, and an error raised after the mark
still propagates. A nested scope discards only its own writes where the backend has savepoints,
and refuses up front where its nested scopes share the root's transaction.

Used by:

- ``tests/unit/test_forze_mock/test_mock_tx_rollback_only_conformance.py`` (journal and strict)
- ``tests/integration/test_forze_postgres/test_pg_tx_rollback_only_conformance.py``
- ``tests/integration/test_forze_mongo/test_mongo_tx_rollback_only_conformance.py``
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

import attrs
import pytest

from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #


class _Boom(RuntimeError):
    """A real error raised after the mark."""


@attrs.define(slots=True, kw_only=True, frozen=True)
class RollbackOnlyHarness:
    """One backend's seam for the rollback-only battery."""

    ctx: ExecutionContext
    """The session the scopes open on."""

    route: str
    """The transaction route."""

    write: Callable[[str], Awaitable[None]]
    """Write one *key* through the backend, inside whatever transaction is open."""

    stored: Callable[[], Awaitable[set[str]]]
    """The keys committed so far, read outside any transaction."""

    savepoints: bool
    """Whether a nested scope is a savepoint the backend can roll back on its own."""

    caplog: pytest.LogCaptureFixture
    """Captures the records every check asserts carry no error."""

    backend: str
    """Label used in assertion messages."""


Check = Callable[[RollbackOnlyHarness], Any]
"""One battery check."""


def _errors(h: RollbackOnlyHarness) -> list[str]:
    return [r.getMessage() for r in h.caplog.records if r.levelno >= logging.ERROR]


# ....................... #


async def check_a_root_rollback_only_discards_its_writes(h: RollbackOnlyHarness) -> None:
    """The dry run: the body runs to the end, nothing is kept, and nothing logs an error."""

    async with h.ctx.tx_ctx.scope(h.route) as tx:
        await h.write("dry")
        tx.rollback_only()

    assert await h.stored() == set(), h.backend
    assert _errors(h) == [], h.backend

    # The next transaction on the same session commits as usual.
    async with h.ctx.tx_ctx.scope(h.route):
        await h.write("real")

    assert await h.stored() == {"real"}, h.backend


async def check_a_nested_rollback_only_discards_only_its_writes(h: RollbackOnlyHarness) -> None:
    """A savepoint rolls back alone; without savepoints the mark is refused, never ignored."""

    if not h.savepoints:
        with pytest.raises(CoreException) as caught:
            async with h.ctx.tx_ctx.scope(h.route):
                await h.write("outer")

                async with h.ctx.tx_ctx.scope(h.route) as nested:
                    await h.write("inner")
                    nested.rollback_only()

        assert caught.value.kind is ExceptionKind.PRECONDITION, h.backend
        assert caught.value.code == "tx_nested_rollback_unsupported", h.backend
        # The refusal propagated through the root, which rolled the whole transaction back.
        assert await h.stored() == set(), h.backend
        return

    async with h.ctx.tx_ctx.scope(h.route):
        await h.write("outer")

        async with h.ctx.tx_ctx.scope(h.route) as nested:
            await h.write("inner")
            nested.rollback_only()

        await h.write("after")

    assert await h.stored() == {"outer", "after"}, h.backend
    assert _errors(h) == [], h.backend


async def check_after_commit_callbacks_do_not_run(h: RollbackOnlyHarness) -> None:
    """Nothing committed, so nothing runs after the commit, as on any rollback."""

    ran: list[str] = []

    async def callback() -> None:
        ran.append("ran")

    async with h.ctx.tx_ctx.scope(h.route) as tx:
        await h.write("dry")
        await h.ctx.tx_ctx.run_or_defer(callback)
        tx.rollback_only()

    assert ran == [], h.backend
    assert await h.stored() == set(), h.backend


async def check_an_error_after_the_mark_still_propagates(h: RollbackOnlyHarness) -> None:
    """The mark never swallows an error: it propagates, and the scope rolls back."""

    with pytest.raises(_Boom):
        async with h.ctx.tx_ctx.scope(h.route) as tx:
            await h.write("dry")
            tx.rollback_only()
            raise _Boom

    assert await h.stored() == set(), h.backend


TX_ROLLBACK_ONLY_BATTERY: tuple[Check, ...] = (
    check_a_root_rollback_only_discards_its_writes,
    check_a_nested_rollback_only_discards_only_its_writes,
    check_after_commit_callbacks_do_not_run,
    check_an_error_after_the_mark_still_propagates,
)
"""Every check, for a backend's module to parametrize over."""
