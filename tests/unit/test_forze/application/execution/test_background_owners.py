"""Background-owner registry: closes registered owners, isolates failures, bounded."""

from __future__ import annotations

import asyncio

from forze.application.execution.context.background import BackgroundOwners

# ----------------------- #


class _Owner:
    def __init__(self, *, fail: bool = False, hang: bool = False) -> None:
        self.fail = fail
        self.hang = hang
        self.closed = False

    async def aclose(self) -> None:
        if self.hang:
            await asyncio.Event().wait()  # never returns on its own

        self.closed = True

        if self.fail:
            raise RuntimeError("aclose boom")


class TestBackgroundOwners:
    async def test_close_is_a_noop_when_empty(self) -> None:
        assert await BackgroundOwners().close(grace=1.0) == 0

    async def test_close_calls_aclose_on_each_owner(self) -> None:
        reg = BackgroundOwners()
        a, b = _Owner(), _Owner()
        reg.register(a)
        reg.register(b)

        assert await reg.close(grace=1.0) == 2
        assert a.closed and b.closed

    async def test_close_isolates_owner_failures(self) -> None:
        reg = BackgroundOwners()
        boom, ok = _Owner(fail=True), _Owner()
        reg.register(boom)
        reg.register(ok)

        # A failing aclose neither blocks the other owner nor raises out of close.
        assert await reg.close(grace=1.0) == 2
        assert ok.closed

    async def test_close_is_bounded_by_grace(self) -> None:
        reg = BackgroundOwners()
        hung = _Owner(hang=True)  # keep a strong ref so the weak registry retains it
        reg.register(hung)

        # A wedged aclose does not hang shutdown — close returns once the grace elapses.
        assert await reg.close(grace=0.01) == 1

    async def test_registry_holds_owners_weakly(self) -> None:
        import gc
        import weakref

        reg = BackgroundOwners()
        owner = _Owner()
        ref = weakref.ref(owner)
        reg.register(owner)

        del owner
        gc.collect()

        # A collected owner (no live background work keeping it alive) drops out silently.
        assert ref() is None
        assert await reg.close(grace=1.0) == 0
