"""Port interception seam — a composable chain wraps port calls under simulation.

Real adapters suspend on I/O; the in-memory mocks don't, so without a yield two concurrent
operations would run as if atomic and interleaving bugs would hide. The
:class:`CooperativeInterceptor` yields at each port boundary; production registers no
interceptors (the resolved port is returned bare and these proxies never apply). The chain
feeds from two surfaces — deps-scoped (``wrap_intercepted`` here) and ambient
(``bind_interceptors``, run-scoped for drivers like ``run_simulation``).
"""

from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator

import attrs
import pytest

from forze.application.execution.interception import (
    CooperativeInterceptor,
    PortCall,
    PortNext,
    bind_interceptors,
    wrap_intercepted,
)
from forze.base.exceptions import CoreException, exc

# ----------------------- #


@attrs.define(slots=True)
class _FakePort:
    order: list[str] = attrs.field(factory=list)

    async def step(self, tag: str) -> str:
        self.order.append(tag)
        return tag

    async def stream(self, n: int) -> AsyncIterator[int]:
        for i in range(n):
            yield i

    def sync_op(self, x: int) -> int:
        return x + 1


# ....................... #


async def _interleaving_order(port: Any) -> list[str]:
    async def worker(tag: str) -> None:
        await port.step(f"{tag}1")
        await port.step(f"{tag}2")

    await asyncio.gather(worker("a"), worker("b"))
    return port.order


def test_no_interceptor_runs_operations_atomically() -> None:
    # Bare port: each worker runs both port calls before the next starts (no yield point).
    port = _FakePort()
    assert asyncio.run(_interleaving_order(port)) == ["a1", "a2", "b1", "b2"]


def test_cooperative_interceptor_interleaves_at_port_boundary() -> None:
    # Deps-scoped chain: the cooperative yield at each port call lets the workers interleave.
    port = wrap_intercepted(
        _FakePort(),
        interceptors=(CooperativeInterceptor(),),
        surface="fake",
        route=None,
    )
    assert asyncio.run(_interleaving_order(port)) == ["a1", "b1", "a2", "b2"]


def test_ambient_binding_feeds_the_chain() -> None:
    # Deps-scoped chain empty; the ambient (run-scoped) binding supplies the interceptor.
    port = wrap_intercepted(_FakePort(), interceptors=(), surface="fake", route=None)

    async def run() -> list[str]:
        with bind_interceptors(CooperativeInterceptor()):
            return await _interleaving_order(port)

    assert asyncio.run(run()) == ["a1", "b1", "a2", "b2"]


def test_interceptors_run_in_registration_order() -> None:
    log: list[str] = []

    @attrs.define(slots=True, frozen=True)
    class _Tag:
        name: str

        async def around(self, call: PortCall, nxt: PortNext) -> Any:
            log.append(f"{self.name}:before")
            result = await nxt(call)
            log.append(f"{self.name}:after")
            return result

    port = wrap_intercepted(
        _FakePort(),
        interceptors=(_Tag("outer"), _Tag("inner")),
        surface="fake",
        route=None,
    )

    async def run() -> None:
        await port.step("x")

    asyncio.run(run())
    # First registered = outermost.
    assert log == ["outer:before", "inner:before", "inner:after", "outer:after"]


def test_async_gen_around_only_interceptor_is_acquisition_only() -> None:
    # An ``around``-only interceptor keeps the historical behavior: one interception at
    # iterator acquisition, not one per yielded item.
    seen: list[str] = []

    @attrs.define(slots=True, frozen=True)
    class _Count:
        async def around(self, call: PortCall, nxt: PortNext) -> Any:
            seen.append(call.op)
            return await nxt(call)

    port = wrap_intercepted(
        _FakePort(), interceptors=(_Count(),), surface="fake", route=None
    )

    async def run() -> list[int]:
        return [i async for i in port.stream(3)]

    assert asyncio.run(run()) == [0, 1, 2]
    assert seen == ["stream"]  # once at the start, not per yielded item


def test_cooperative_interceptor_interleaves_per_stream_item() -> None:
    # A stream-aware interceptor yields per item, so two workers consuming streams
    # concurrently interleave item-by-item instead of each draining its stream atomically.
    order: list[str] = []

    @attrs.define(slots=True)
    class _StreamPort:
        tag: str

        async def stream(self, n: int) -> AsyncIterator[int]:
            for i in range(n):
                order.append(f"{self.tag}{i}")
                yield i

    def _wrap(tag: str) -> Any:
        return wrap_intercepted(
            _StreamPort(tag),
            interceptors=(CooperativeInterceptor(),),
            surface="fake",
            route=None,
        )

    async def worker(port: Any) -> None:
        async for _ in port.stream(2):
            pass

    async def run() -> list[str]:
        await asyncio.gather(worker(_wrap("a")), worker(_wrap("b")))
        return order

    # Per-item yield -> interleaved. (An acquisition-only interceptor would give a0,a1,b0,b1.)
    assert asyncio.run(run()) == ["a0", "b0", "a1", "b1"]


def test_stream_aware_interceptor_sees_and_transforms_each_item() -> None:
    from typing import AsyncIterator as _AsyncIterator

    from forze.application.execution.interception import StreamPortNext

    seen: list[int] = []

    @attrs.define(slots=True, frozen=True)
    class _Doubler:
        async def around_stream(
            self, call: PortCall, nxt: StreamPortNext
        ) -> _AsyncIterator[Any]:
            async for item in nxt(call):
                seen.append(item)
                yield item * 10

    port = wrap_intercepted(
        _FakePort(), interceptors=(_Doubler(),), surface="fake", route=None
    )

    async def run() -> list[int]:
        return [i async for i in port.stream(3)]

    assert asyncio.run(run()) == [0, 10, 20]  # each item transformed
    assert seen == [0, 1, 2]  # the interceptor saw every item, not just acquisition


def test_mixed_chain_stream_aware_and_acquisition_only() -> None:
    # A stream-aware interceptor (per-item) composed with an around-only interceptor
    # (acquisition-only, via ``_acquisition_only_stream``): the stream still transforms per
    # item while the outer around-only wraps acquisition exactly once, before any item flows.
    from typing import AsyncIterator as _AsyncIterator

    from forze.application.execution.interception import StreamPortNext

    events: list[str] = []

    @attrs.define(slots=True, frozen=True)
    class _Doubler:
        async def around_stream(
            self, call: PortCall, nxt: StreamPortNext
        ) -> _AsyncIterator[Any]:
            async for item in nxt(call):
                events.append(f"item:{item}")
                yield item * 10

    @attrs.define(slots=True, frozen=True)
    class _AroundOnly:
        async def around(self, call: PortCall, nxt: PortNext) -> Any:
            events.append("acquire")
            return await nxt(call)

    # First = outermost: the around-only wraps acquisition around the stream-aware inner one.
    port = wrap_intercepted(
        _FakePort(),
        interceptors=(_AroundOnly(), _Doubler()),
        surface="fake",
        route=None,
    )

    async def run() -> list[int]:
        return [i async for i in port.stream(3)]

    # The stream-aware transform survives the mixed chain...
    assert asyncio.run(run()) == [0, 10, 20]
    # ...and acquisition ran once, before any item — the around-only is acquisition-only.
    assert events == ["acquire", "item:0", "item:1", "item:2"]


def test_sync_method_passes_through_uninterceptable() -> None:
    seen: list[str] = []

    @attrs.define(slots=True, frozen=True)
    class _Count:
        async def around(self, call: PortCall, nxt: PortNext) -> Any:
            seen.append(call.op)
            return await nxt(call)

    port = wrap_intercepted(
        _FakePort(), interceptors=(_Count(),), surface="fake", route=None
    )
    assert port.sync_op(1) == 2
    assert seen == []  # sync methods are not intercepted (interceptors model async I/O)


def test_latency_model_receives_call_dimensions() -> None:
    seen: list[tuple[str | None, str | None, str]] = []

    def latency(surface: str | None, route: str | None, op: str) -> float:
        seen.append((surface, route, op))
        return 0.0

    port = wrap_intercepted(
        _FakePort(),
        interceptors=(CooperativeInterceptor(latency=latency),),
        surface="docs",
        route="orders",
    )

    async def run() -> None:
        await port.step("x")

    asyncio.run(run())
    assert seen == [("docs", "orders", "step")]


def test_interceptor_can_rewrite_awaitable_call_args() -> None:
    # An interceptor that rewrites the call must have the real port see the rewritten args
    # (the terminal honors the continuation's PortCall, not the original closure args).
    @attrs.define(slots=True, frozen=True)
    class _Upper:
        async def around(self, call: PortCall, nxt: PortNext) -> Any:
            return await nxt(attrs.evolve(call, args=(call.args[0].upper(),)))

    fake = _FakePort()
    port = wrap_intercepted(fake, interceptors=(_Upper(),), surface="fake", route=None)

    assert asyncio.run(port.step("hi")) == "HI"  # terminal used the rewritten arg
    assert fake.order == ["HI"]  # the real port saw the rewritten value


@attrs.define(slots=True)
class _CursorPort:
    """A port whose stream tracks its own teardown, like a driver cursor would."""

    events: list[str] = attrs.field(factory=list)

    async def stream(self, n: int) -> AsyncIterator[int]:
        try:
            for i in range(n):
                yield i
        finally:
            self.events.append("backend:closed")


def test_async_gen_aclose_closes_backend_stream_before_scope_exit() -> None:
    # Closing the intercepted stream must close the backend generator at that moment —
    # its ``finally`` (the cursor release) runs before the consumer's scope exits, not
    # whenever GC finalizes an abandoned generator.
    fake = _CursorPort()
    port = wrap_intercepted(fake, interceptors=(), surface="fake", route=None)

    async def run() -> None:
        stream = port.stream(10)
        assert await anext(stream) == 0
        await stream.aclose()
        fake.events.append("scope:exit")

    asyncio.run(run())
    assert fake.events == ["backend:closed", "scope:exit"]


def test_async_gen_aclose_propagates_through_mixed_chain() -> None:
    # Close chains through every wrapper — acquisition-only, stream-aware
    # (CooperativeInterceptor), and the proxy itself — down to the backend generator.
    @attrs.define(slots=True, frozen=True)
    class _AroundOnly:
        async def around(self, call: PortCall, nxt: PortNext) -> Any:
            return await nxt(call)

    fake = _CursorPort()
    port = wrap_intercepted(
        fake,
        interceptors=(_AroundOnly(), CooperativeInterceptor()),
        surface="fake",
        route=None,
    )

    async def run() -> None:
        stream = port.stream(10)
        assert await anext(stream) == 0
        await stream.aclose()
        fake.events.append("scope:exit")

    asyncio.run(run())
    assert fake.events == ["backend:closed", "scope:exit"]


def test_async_gen_full_consumption_closes_backend_once() -> None:
    # Natural exhaustion runs the backend ``finally`` exactly once; a later ``aclose`` on
    # the exhausted stream is a no-op (no double-close error, no second teardown).
    fake = _CursorPort()
    port = wrap_intercepted(
        fake, interceptors=(CooperativeInterceptor(),), surface="fake", route=None
    )

    async def run() -> list[int]:
        stream = port.stream(3)
        got = [i async for i in stream]
        await stream.aclose()
        return got

    assert asyncio.run(run()) == [0, 1, 2]
    assert fake.events == ["backend:closed"]


def test_async_gen_consumer_athrow_closes_backend_and_propagates() -> None:
    # An exception thrown in by the consumer surfaces from the outer stream and still
    # closes the backend generator on the way out.
    fake = _CursorPort()
    port = wrap_intercepted(
        fake, interceptors=(CooperativeInterceptor(),), surface="fake", route=None
    )

    async def run() -> None:
        stream = port.stream(10)
        assert await anext(stream) == 0

        with pytest.raises(ValueError, match="stop"):
            await stream.athrow(ValueError("stop"))

        fake.events.append("scope:exit")

    asyncio.run(run())
    assert fake.events == ["backend:closed", "scope:exit"]


def test_async_gen_inner_failure_surfaces_intact_and_closes_backend() -> None:
    # A mid-stream backend failure keeps its type/kind through the chain and the backend
    # ``finally`` still runs deterministically.
    events: list[str] = []

    @attrs.define(slots=True)
    class _FailingPort:
        async def stream(self, n: int) -> AsyncIterator[int]:
            try:
                yield 0
                raise exc.not_found("gone")
            finally:
                events.append("backend:closed")

    port = wrap_intercepted(
        _FailingPort(),
        interceptors=(CooperativeInterceptor(),),
        surface="fake",
        route=None,
    )

    async def run() -> list[int]:
        got: list[int] = []

        with pytest.raises(CoreException) as excinfo:
            async for i in port.stream(10):
                got.append(i)

        assert excinfo.value.kind.value == "not_found"
        events.append("scope:exit")
        return got

    assert asyncio.run(run()) == [0]
    assert events == ["backend:closed", "scope:exit"]


def test_cooperative_around_stream_closes_inner_on_aclose() -> None:
    # The builtin stream-aware interceptor, taken alone: closing its stream closes the
    # inner continuation at that moment.
    events: list[str] = []

    async def inner(_call: PortCall) -> AsyncIterator[int]:
        try:
            yield 1
            yield 2
        finally:
            events.append("inner:closed")

    call = PortCall(surface="fake", route=None, op="stream", args=(), kwargs={})

    async def run() -> None:
        stream = CooperativeInterceptor().around_stream(call, inner)
        assert await anext(stream) == 1
        await stream.aclose()
        events.append("scope:exit")

    asyncio.run(run())
    assert events == ["inner:closed", "scope:exit"]


def test_interceptor_can_rewrite_async_gen_call_args() -> None:
    @attrs.define(slots=True, frozen=True)
    class _DropOne:
        async def around(self, call: PortCall, nxt: PortNext) -> Any:
            return await nxt(attrs.evolve(call, args=(call.args[0] - 1,)))

    port = wrap_intercepted(
        _FakePort(), interceptors=(_DropOne(),), surface="fake", route=None
    )

    async def run() -> list[int]:
        return [i async for i in port.stream(3)]

    assert asyncio.run(run()) == [0, 1]  # rewritten 3 -> 2
