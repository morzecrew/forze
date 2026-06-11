"""Unit tests for :class:`~forze.base.primitives.context_scope.ContextScopedResource`."""

import asyncio

import pytest

from forze.base.primitives.context_scope import ContextScopedResource

# ----------------------- #


class _Resource:
    def __init__(self, name: str) -> None:
        self.name = name
        self.closed = False


# ....................... #


class _Harness:
    def __init__(self) -> None:
        self.scope: ContextScopedResource[_Resource] = ContextScopedResource("test")
        self.created: list[_Resource] = []
        self.closed: list[_Resource] = []

    # ....................... #

    async def create(self) -> _Resource:
        resource = _Resource(f"r{len(self.created)}")
        self.created.append(resource)

        return resource

    # ....................... #

    async def close(self, resource: _Resource) -> None:
        resource.closed = True
        self.closed.append(resource)


# ----------------------- #


class TestContextScopedResource:
    async def test_outermost_creates_and_closes(self) -> None:
        h = _Harness()

        async with h.scope.scope(h.create, closer=h.close) as resource:
            assert resource is h.created[0]
            assert h.scope.current() is resource
            assert not resource.closed

        assert resource.closed
        assert h.closed == [resource]
        assert h.scope.current() is None

    # ....................... #

    async def test_nested_scopes_reuse_outermost_resource(self) -> None:
        h = _Harness()

        async with h.scope.scope(h.create, closer=h.close) as outer:
            async with h.scope.scope(h.create, closer=h.close) as inner:
                assert inner is outer

                async with h.scope.scope(h.create, closer=h.close) as innermost:
                    assert innermost is outer

            assert not outer.closed  # inner exits never close

        assert len(h.created) == 1
        assert h.closed == [outer]

    # ....................... #

    async def test_reusable_predicate_rejects_stale_resource(self) -> None:
        h = _Harness()

        async with h.scope.scope(h.create, closer=h.close) as outer:
            outer.closed = True  # simulate e.g. a closed channel

            async with h.scope.scope(
                h.create,
                closer=h.close,
                reusable=lambda r: not r.closed,
            ) as inner:
                assert inner is not outer
                assert h.scope.current() is inner

            # inner binding torn down, outer binding restored
            assert h.scope.current() is outer

        assert len(h.created) == 2
        assert h.closed[0] is h.created[1]

    # ....................... #

    async def test_exception_in_body_still_resets_and_closes(self) -> None:
        h = _Harness()

        with pytest.raises(RuntimeError, match="boom"):
            async with h.scope.scope(h.create, closer=h.close):
                raise RuntimeError("boom")

        assert h.scope.current() is None
        assert h.closed == h.created

        # a fresh scope creates a new resource (tokens were reset)
        async with h.scope.scope(h.create, closer=h.close) as resource:
            assert resource is h.created[1]

    # ....................... #

    async def test_closer_error_never_masks_body_exception(self) -> None:
        h = _Harness()

        async def bad_closer(resource: _Resource) -> None:
            raise OSError("close failed")

        # Body raised: the body's exception propagates, closer error suppressed.
        with pytest.raises(RuntimeError, match="boom"):
            async with h.scope.scope(h.create, closer=bad_closer):
                raise RuntimeError("boom")

        assert h.scope.current() is None

    # ....................... #

    async def test_closer_error_propagates_on_happy_path(self) -> None:
        h = _Harness()

        async def bad_closer(resource: _Resource) -> None:
            raise OSError("close failed")

        with pytest.raises(OSError, match="close failed"):
            async with h.scope.scope(h.create, closer=bad_closer):
                pass

        # Tokens were reset before the closer ran.
        assert h.scope.current() is None

    # ....................... #

    async def test_no_closer_keeps_resource_open(self) -> None:
        h = _Harness()

        async with h.scope.scope(h.create) as resource:
            pass

        assert not resource.closed
        assert h.closed == []
        assert h.scope.current() is None

    # ....................... #

    async def test_concurrent_tasks_have_independent_scopes(self) -> None:
        h = _Harness()
        seen: dict[str, _Resource] = {}

        async def worker(tag: str) -> None:
            async with h.scope.scope(h.create, closer=h.close) as resource:
                await asyncio.sleep(0.01)
                assert h.scope.current() is resource
                seen[tag] = resource

        await asyncio.gather(worker("a"), worker("b"))

        assert seen["a"] is not seen["b"]
        assert len(h.created) == 2
        assert set(h.closed) == set(h.created)
