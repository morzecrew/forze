"""Unit tests for forze.application.execution.runtime."""

import asyncio
import secrets
import threading
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import pytest

from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import Deps, DepsRegistry
from forze.application.execution.lifecycle import LifecyclePlan
from forze.application.execution.runtime import ExecutionRuntime
from forze.base.exceptions import CoreException

# ----------------------- #


class TestExecutionRuntime:
    """Tests for ExecutionRuntime."""

    def test_create_context_builds_from_deps_registry(self) -> None:
        registration = Deps.plain({})
        rt = ExecutionRuntime(deps=DepsRegistry.from_deps(registration).freeze())
        rt.create_context()
        ctx = rt.get_context()
        assert ctx is not None
        assert ctx.deps.store.plain_deps == registration.store.plain_deps
        assert ctx.deps.store.routed_deps == registration.store.routed_deps

    def test_get_context_outside_scope_raises(self) -> None:

        rt = ExecutionRuntime()
        with pytest.raises(CoreException, match="not set"):
            rt.get_context()

    @pytest.mark.asyncio
    async def test_scope_runs_startup_and_shutdown(self) -> None:
        order: list[str] = []

        async def start(ctx):
            order.append("start")

        async def shut(ctx):
            order.append("shut")

        step = LifecycleStep(id="s", startup=start, shutdown=shut)
        plan = LifecyclePlan.from_steps(step).freeze()
        rt = ExecutionRuntime(lifecycle=plan)

        async with rt.scope():
            assert rt.get_context() is not None
            assert order == ["start"]

        assert order == ["start", "shut"]

    @pytest.mark.asyncio
    async def test_scope_binds_cursor_signer_and_restores(self) -> None:
        from forze.application.contracts.querying import (
            CursorTokenSigner,
            current_cursor_signer,
        )

        signer = CursorTokenSigner(secret=b"k" * 32)
        rt = ExecutionRuntime(cursor_token_signer=signer)

        assert current_cursor_signer() is None

        async with rt.scope():
            # Operations in this scope mint/verify cursor tokens with this runtime's signer.
            assert current_cursor_signer() is signer

        assert current_cursor_signer() is None  # restored on scope exit

    @pytest.mark.asyncio
    async def test_scope_without_signer_leaves_cursor_signing_off(self) -> None:
        from forze.application.contracts.querying import current_cursor_signer

        async with ExecutionRuntime().scope():
            assert current_cursor_signer() is None

    @pytest.mark.asyncio
    async def test_scope_binds_cursor_cipher_and_restores(self) -> None:
        from forze.application.contracts.querying import (
            CursorTokenCipher,
            current_cursor_cipher,
        )

        cipher = CursorTokenCipher(secret=b"z" * 32)
        rt = ExecutionRuntime(cursor_token_cipher=cipher)

        assert current_cursor_cipher() is None

        async with rt.scope():
            # Operations in this scope mint/verify encrypted cursor tokens with this cipher.
            assert current_cursor_cipher() is cipher

        assert current_cursor_cipher() is None  # restored on scope exit

    @pytest.mark.asyncio
    async def test_scope_builds_lifecycle_from_modules(self) -> None:
        order: list[str] = []

        async def start(_ctx) -> None:
            order.append("start")

        class _Module:
            def __call__(self) -> tuple[LifecycleStep, ...]:
                return (LifecycleStep(id="s", startup=start),)

        rt = ExecutionRuntime(lifecycle=LifecyclePlan.from_modules(_Module()).freeze())

        async with rt.scope():
            assert order == ["start"]

    @pytest.mark.asyncio
    async def test_scope_failed_startup_shuts_started_steps_down_exactly_once(
        self,
    ) -> None:
        startups: dict[str, int] = {}
        shutdowns: dict[str, int] = {}

        def _step(i: int, *, fail: bool = False) -> LifecycleStep:
            name = f"s{i}"

            async def up(_ctx) -> None:
                startups[name] = startups.get(name, 0) + 1
                if fail:
                    raise RuntimeError("startup failed at s3")

            async def down(_ctx) -> None:
                shutdowns[name] = shutdowns.get(name, 0) + 1

            return LifecycleStep(
                id=name,
                startup=up,
                shutdown=down,
                depends_on=(f"s{i - 1}",) if i > 1 else (),
            )

        plan = LifecyclePlan.from_steps(
            _step(1),
            _step(2),
            _step(3, fail=True),
            _step(4),
            _step(5),
        ).freeze()
        rt = ExecutionRuntime(lifecycle=plan)

        with pytest.raises(RuntimeError, match="startup failed at s3"):
            async with rt.scope():
                pass  # pragma: no cover - never reached

        # Steps 1-2 started and were shut down exactly once (by the rollback);
        # the scope-exit shutdown did not run them a second time.
        assert startups == {"s1": 1, "s2": 1, "s3": 1}
        assert shutdowns == {"s1": 1, "s2": 1}

    @pytest.mark.asyncio
    async def test_scope_shuts_every_started_step_down_exactly_once(self) -> None:
        shutdowns: dict[str, int] = {}

        def _step(i: int) -> LifecycleStep:
            name = f"s{i}"

            async def down(_ctx) -> None:
                shutdowns[name] = shutdowns.get(name, 0) + 1

            return LifecycleStep(
                id=name,
                shutdown=down,
                depends_on=(f"s{i - 1}",) if i > 1 else (),
            )

        plan = LifecyclePlan.from_steps(_step(1), _step(2), _step(3)).freeze()
        rt = ExecutionRuntime(lifecycle=plan)

        async with rt.scope():
            pass

        assert shutdowns == {"s1": 1, "s2": 1, "s3": 1}

    @pytest.mark.asyncio
    async def test_scope_resets_context_on_exit(self) -> None:

        rt = ExecutionRuntime(deps=DepsRegistry().freeze())
        async with rt.scope():
            ctx = rt.get_context()
            assert ctx is not None

        with pytest.raises(CoreException, match="not set"):
            rt.get_context()


class TestScopeCpuExecutor:
    """The scope binds an optional CPU-offload executor and closes it on exit."""

    @pytest.mark.asyncio
    async def test_scope_binds_executor_and_run_cpu_uses_it(self) -> None:
        from forze.base.primitives import (
            ThreadPoolCpuExecutor,
            current_cpu_executor,
            run_cpu,
        )

        ex = ThreadPoolCpuExecutor(max_workers=2)
        rt = ExecutionRuntime(deps=DepsRegistry().freeze(), cpu_executor=ex)

        async with rt.scope():
            assert current_cpu_executor() is ex
            assert await run_cpu(lambda: 6 * 7) == 42

    @pytest.mark.asyncio
    async def test_scope_does_not_close_caller_owned_executor(self) -> None:
        from forze.base.primitives import ThreadPoolCpuExecutor, current_cpu_executor, run_cpu

        ex = ThreadPoolCpuExecutor(max_workers=2)
        rt = ExecutionRuntime(deps=DepsRegistry().freeze(), cpu_executor=ex)

        async with rt.scope():
            await run_cpu(lambda: None)  # force the pool to materialize
            assert ex._pool is not None

        # The caller owns the executor: the runtime binds it but must NOT close it;
        # only the ambient binding is restored to the default.
        assert ex._pool is not None
        assert current_cpu_executor() is not ex
        ex.close()  # caller cleans up

    @pytest.mark.asyncio
    async def test_unbound_scope_owns_and_closes_a_pool(self) -> None:
        from forze.base.primitives import (
            ThreadPoolCpuExecutor,
            current_cpu_executor,
            run_cpu,
        )

        rt = ExecutionRuntime(deps=DepsRegistry().freeze())

        async with rt.scope():
            owned = current_cpu_executor()
            # With nothing bound, the runtime owns a scope-lifetime thread pool.
            assert isinstance(owned, ThreadPoolCpuExecutor)
            assert await run_cpu(lambda: 6 * 7) == 42  # materializes the pool
            assert owned._pool is not None

        # The runtime-owned pool is closed on scope exit (unlike a caller-injected one).
        assert owned._pool is None

    @pytest.mark.asyncio
    async def test_ambient_executor_is_not_overridden(self) -> None:
        from forze.base.primitives import (
            InlineCpuExecutor,
            bind_cpu_executor,
            current_cpu_executor,
        )

        ambient = InlineCpuExecutor()
        rt = ExecutionRuntime(deps=DepsRegistry().freeze())

        # An executor already bound around the scope (e.g. a simulation's) wins: the
        # runtime defers to it and never binds a pool of its own.
        with bind_cpu_executor(ambient):
            async with rt.scope():
                assert current_cpu_executor() is ambient

    @pytest.mark.asyncio
    async def test_cpu_workers_sizes_the_owned_pool(self) -> None:
        from forze.base.primitives import ThreadPoolCpuExecutor, current_cpu_executor

        rt = ExecutionRuntime(deps=DepsRegistry().freeze(), cpu_workers=3)

        async with rt.scope():
            owned = current_cpu_executor()
            assert isinstance(owned, ThreadPoolCpuExecutor)
            assert owned.max_workers == 3

    def test_build_runtime_threads_cpu_executor(self) -> None:
        from forze.application.execution import build_runtime
        from forze.base.primitives import InlineCpuExecutor

        ex = InlineCpuExecutor()
        rt = build_runtime(cpu_executor=ex)
        assert rt.cpu_executor is ex

    @pytest.mark.parametrize("field", ["cpu_workers", "isolated_cpu_workers"])
    def test_non_positive_worker_count_is_refused(self, field: str) -> None:
        with pytest.raises(CoreException) as ei:
            ExecutionRuntime(deps=DepsRegistry().freeze(), **{field: 0})

        assert ei.value.code == f"core.runtime.{field}_invalid"
        assert field in str(ei.value)

    @pytest.mark.asyncio
    async def test_unbound_scope_owns_and_closes_an_isolated_process_pool(self) -> None:
        import os

        from forze.base.primitives import (
            ProcessPoolCpuExecutor,
            current_isolated_cpu_executor,
            run_cpu,
        )

        rt = ExecutionRuntime(deps=DepsRegistry().freeze(), isolated_cpu_workers=1)

        async with rt.scope():
            owned = current_isolated_cpu_executor()
            assert isinstance(owned, ProcessPoolCpuExecutor)
            assert owned.max_workers == 1
            assert await run_cpu(os.getpid, isolated=True) != os.getpid()

        assert owned._pool is None  # closed with the scope
        assert current_isolated_cpu_executor() is None

    @pytest.mark.asyncio
    async def test_scope_binds_but_does_not_close_an_injected_isolated_executor(self) -> None:
        from forze.base.primitives import (
            InlineCpuExecutor,
            ThreadPoolCpuExecutor,
            current_isolated_cpu_executor,
        )

        injected = InlineCpuExecutor()
        default = ThreadPoolCpuExecutor(max_workers=1)
        rt = ExecutionRuntime(
            deps=DepsRegistry().freeze(), cpu_executor=default, isolated_cpu_executor=injected
        )

        async with rt.scope():
            assert current_isolated_cpu_executor() is injected

        assert current_isolated_cpu_executor() is None

    @pytest.mark.asyncio
    async def test_ambient_isolated_executor_is_not_overridden(self) -> None:
        from forze.base.primitives import (
            InlineCpuExecutor,
            bind_isolated_cpu_executor,
            current_isolated_cpu_executor,
        )

        ambient = InlineCpuExecutor()
        rt = ExecutionRuntime(deps=DepsRegistry().freeze())

        with bind_isolated_cpu_executor(ambient):
            async with rt.scope():
                assert current_isolated_cpu_executor() is ambient

    def test_build_runtime_threads_isolated_cpu_settings(self) -> None:
        from forze.application.execution import build_runtime
        from forze.base.primitives import InlineCpuExecutor

        ex = InlineCpuExecutor()
        rt = build_runtime(isolated_cpu_executor=ex, isolated_cpu_workers=2)
        assert rt.isolated_cpu_executor is ex
        assert rt.isolated_cpu_workers == 2


# ....................... #


@asynccontextmanager
async def _held_by_lifespan(
    *runtimes: ExecutionRuntime,
) -> AsyncIterator[Callable[[int], Awaitable[None]]]:
    """Hold each runtime's scope open in a task of its own, as an ASGI server runs a lifespan.

    The caller then plays the server: it runs requests in its own context, which no scope's
    task ever bound anything in — uvicorn creates request tasks from the server's context.
    It gets a callable that exits the *i*-th scope early; every scope exits on the way out,
    whatever happened, so no test leaks a process-wide hold into the next.
    """

    stops: list[asyncio.Event] = []
    lifespans: list[asyncio.Task[None]] = []

    async def lifespan(runtime: ExecutionRuntime, started: asyncio.Event, stop: asyncio.Event):
        async with runtime.scope():
            started.set()
            await stop.wait()

    async def exit_scope(i: int) -> None:
        stops[i].set()
        await lifespans[i]

    try:
        for runtime in runtimes:
            started = asyncio.Event()
            stops.append(asyncio.Event())
            lifespans.append(asyncio.create_task(lifespan(runtime, started, stops[-1])))
            await started.wait()

        yield exit_scope

    finally:
        for stop in stops:
            stop.set()

        await asyncio.gather(*lifespans)


def _token() -> str:
    from forze.application.contracts.querying import encode_keyset_v1

    return encode_keyset_v1(sort_keys=["id"], directions=["asc"], values=[1])


def _signed(token: str) -> bool:
    return "." in token


class TestScopeBindingsReachRequests:
    """The scope's bindings reach code running outside its task — an ASGI request."""

    @pytest.mark.asyncio
    async def test_run_cpu_in_a_request_runs_on_the_runtime_pool(self) -> None:
        from forze.base.primitives import ThreadPoolCpuExecutor, current_cpu_executor, run_cpu

        async with _held_by_lifespan(ExecutionRuntime()):
            assert isinstance(current_cpu_executor(), ThreadPoolCpuExecutor)
            thread = await run_cpu(lambda: threading.current_thread().name)

        assert thread.startswith("forze-cpu")

    @pytest.mark.asyncio
    async def test_an_injected_executor_reaches_a_request(self) -> None:
        from forze.base.primitives import InlineCpuExecutor, current_cpu_executor

        injected = InlineCpuExecutor()

        async with _held_by_lifespan(ExecutionRuntime(cpu_executor=injected)):
            assert current_cpu_executor() is injected

    @pytest.mark.asyncio
    async def test_bindings_inherited_around_a_scope_stay_in_its_task(self) -> None:
        from forze.application.contracts.querying import (
            CursorTokenCipher,
            CursorTokenSigner,
            bind_cursor_cipher,
            bind_cursor_signer,
            current_cursor_cipher,
            current_cursor_signer,
        )
        from forze.base.primitives import (
            InlineCpuExecutor,
            bind_cpu_executor,
            current_cpu_executor,
        )

        ambient = InlineCpuExecutor()
        signer = CursorTokenSigner(secret=secrets.token_bytes(32))
        cipher = CursorTokenCipher(secret=secrets.token_bytes(32))
        inside: list[object] = []
        stop, started = asyncio.Event(), asyncio.Event()

        async def lifespan() -> None:
            with bind_cpu_executor(ambient), bind_cursor_signer(signer), bind_cursor_cipher(cipher):
                async with ExecutionRuntime().scope():
                    inside.extend(
                        [current_cpu_executor(), current_cursor_signer(), current_cursor_cipher()]
                    )
                    started.set()
                    await stop.wait()

        task = asyncio.create_task(lifespan())

        try:
            await started.wait()

            # The scope's own task defers to what it inherited ...
            assert inside == [ambient, signer, cipher]
            assert inside[0] is ambient

            # ... and never publishes it to a task that binds nothing.
            assert current_cpu_executor() is not ambient
            assert current_cursor_signer() is None
            assert current_cursor_cipher() is None

        finally:
            stop.set()
            await task

    @pytest.mark.asyncio
    async def test_isolated_run_cpu_in_a_request_reaches_the_runtime_process_pool(self) -> None:
        import os

        from forze.base.primitives import (
            ProcessPoolCpuExecutor,
            current_isolated_cpu_executor,
            run_cpu,
        )

        async with _held_by_lifespan(ExecutionRuntime(isolated_cpu_workers=1)):
            assert isinstance(current_isolated_cpu_executor(), ProcessPoolCpuExecutor)
            pid = await run_cpu(os.getpid, isolated=True)

        assert pid != os.getpid()

        # Scope exit releases it: back to the refusal, never a fallback.
        assert current_isolated_cpu_executor() is None

        with pytest.raises(CoreException) as caught:
            await run_cpu(os.getpid, isolated=True)

        assert caught.value.code == "cpu_offload_isolated_unbound"

    @pytest.mark.asyncio
    async def test_an_injected_isolated_executor_reaches_a_request(self) -> None:
        from forze.base.primitives import InlineCpuExecutor, current_isolated_cpu_executor

        injected = InlineCpuExecutor()

        async with _held_by_lifespan(ExecutionRuntime(isolated_cpu_executor=injected)):
            assert current_isolated_cpu_executor() is injected

    @pytest.mark.asyncio
    async def test_an_isolated_executor_inherited_around_a_scope_stays_in_its_task(
        self,
    ) -> None:
        from forze.base.primitives import (
            InlineCpuExecutor,
            bind_isolated_cpu_executor,
            current_isolated_cpu_executor,
        )

        ambient = InlineCpuExecutor()
        inside: list[object] = []
        stop, started = asyncio.Event(), asyncio.Event()

        async def lifespan() -> None:
            with bind_isolated_cpu_executor(ambient):
                async with ExecutionRuntime().scope():
                    inside.append(current_isolated_cpu_executor())
                    started.set()
                    await stop.wait()

        task = asyncio.create_task(lifespan())

        try:
            await started.wait()

            assert inside == [ambient]  # the scope's own task defers to it ...
            assert current_isolated_cpu_executor() is None  # ... and never publishes it

        finally:
            stop.set()
            await task

    @pytest.mark.asyncio
    async def test_cursor_tokens_in_a_request_are_signed(self) -> None:
        from forze.application.contracts.querying import (
            CursorTokenSigner,
            current_cursor_signer,
            decode_keyset_v1,
        )

        signer = CursorTokenSigner(secret=secrets.token_bytes(32))
        unsigned = _token()

        async with _held_by_lifespan(ExecutionRuntime(cursor_token_signer=signer)):
            assert current_cursor_signer() is signer
            assert _signed(_token())

            with pytest.raises(CoreException, match="Invalid cursor token"):
                decode_keyset_v1(unsigned)

    @pytest.mark.asyncio
    async def test_cursor_tokens_in_a_request_are_encrypted(self) -> None:
        from forze.application.contracts.querying import (
            CursorTokenCipher,
            current_cursor_cipher,
            decode_keyset_v1,
        )

        cipher = CursorTokenCipher(secret=secrets.token_bytes(32))
        plaintext = _token()

        async with _held_by_lifespan(ExecutionRuntime(cursor_token_cipher=cipher)):
            assert current_cursor_cipher() is cipher
            assert _token().startswith("~")

            with pytest.raises(CoreException, match="Invalid cursor token"):
                decode_keyset_v1(plaintext)

    @pytest.mark.asyncio
    async def test_scope_exit_withdraws_its_bindings(self) -> None:
        from forze.application.contracts.querying import (
            CursorTokenCipher,
            CursorTokenSigner,
            current_cursor_cipher,
            current_cursor_signer,
        )
        from forze.base.primitives import InlineCpuExecutor, current_cpu_executor

        rt = ExecutionRuntime(
            cursor_token_signer=CursorTokenSigner(secret=secrets.token_bytes(32)),
            cursor_token_cipher=CursorTokenCipher(secret=secrets.token_bytes(32)),
        )

        async with _held_by_lifespan(rt):
            assert current_cursor_signer() is not None

        assert isinstance(current_cpu_executor(), InlineCpuExecutor)
        assert current_cursor_signer() is None
        assert current_cursor_cipher() is None


class TestScopeBindingsWithSeveralRuntimes:
    """Several runtime scopes open: deterministic, and fail closed for the cursor keys."""

    @pytest.mark.asyncio
    async def test_a_request_offloads_to_the_oldest_open_pool(self) -> None:
        from forze.base.primitives import InlineCpuExecutor, current_cpu_executor

        first, second = InlineCpuExecutor(), InlineCpuExecutor()
        runtimes = ExecutionRuntime(cpu_executor=first), ExecutionRuntime(cpu_executor=second)

        async with _held_by_lifespan(*runtimes) as exit_scope:
            assert current_cpu_executor() is first

            await exit_scope(0)
            assert current_cpu_executor() is second

    @pytest.mark.asyncio
    async def test_a_request_isolates_on_the_oldest_open_isolated_executor(self) -> None:
        from forze.base.primitives import InlineCpuExecutor, current_isolated_cpu_executor

        first, second = InlineCpuExecutor(), InlineCpuExecutor()
        runtimes = (
            ExecutionRuntime(isolated_cpu_executor=first),
            ExecutionRuntime(isolated_cpu_executor=second),
        )

        async with _held_by_lifespan(*runtimes) as exit_scope:
            assert current_isolated_cpu_executor() is first

            await exit_scope(0)
            assert current_isolated_cpu_executor() is second

    @pytest.mark.asyncio
    async def test_a_request_refuses_to_pick_between_two_signers(self) -> None:
        from forze.application.contracts.querying import CursorTokenSigner

        a = ExecutionRuntime(cursor_token_signer=CursorTokenSigner(secret=secrets.token_bytes(32)))
        b = ExecutionRuntime(cursor_token_signer=CursorTokenSigner(secret=secrets.token_bytes(32)))

        async with _held_by_lifespan(a, b):
            with pytest.raises(CoreException) as caught:
                _token()

        assert caught.value.code == "cursor_protection_ambiguous"

    @pytest.mark.asyncio
    async def test_a_request_refuses_a_cipher_another_runtime_does_not_use(self) -> None:
        from forze.application.contracts.querying import CursorTokenCipher

        encrypted = ExecutionRuntime(
            cursor_token_cipher=CursorTokenCipher(secret=secrets.token_bytes(32))
        )

        async with _held_by_lifespan(encrypted, ExecutionRuntime()):
            with pytest.raises(CoreException) as caught:
                _token()

        assert caught.value.code == "cursor_protection_ambiguous"

    @pytest.mark.asyncio
    async def test_runtimes_that_agree_on_the_signer_share_it(self) -> None:
        from forze.application.contracts.querying import CursorTokenSigner

        key = secrets.token_bytes(32)
        a = ExecutionRuntime(cursor_token_signer=CursorTokenSigner(secret=key))
        b = ExecutionRuntime(cursor_token_signer=CursorTokenSigner(secret=key))

        async with _held_by_lifespan(a, b):
            assert _signed(_token())

    @pytest.mark.asyncio
    async def test_runtimes_that_agree_on_the_cipher_share_it(self) -> None:
        from forze.application.contracts.querying import CursorTokenCipher

        key = secrets.token_bytes(32)
        a = ExecutionRuntime(cursor_token_cipher=CursorTokenCipher(secret=key))
        b = ExecutionRuntime(cursor_token_cipher=CursorTokenCipher(secret=key))

        async with _held_by_lifespan(a, b):
            assert _token().startswith("~")

    @pytest.mark.asyncio
    async def test_an_agreed_cipher_supersedes_unequal_signers(self) -> None:
        from forze.application.contracts.querying import CursorTokenCipher, CursorTokenSigner

        key = secrets.token_bytes(32)
        a = ExecutionRuntime(
            cursor_token_cipher=CursorTokenCipher(secret=key),
            cursor_token_signer=CursorTokenSigner(secret=secrets.token_bytes(32)),
        )
        b = ExecutionRuntime(
            cursor_token_cipher=CursorTokenCipher(secret=key),
            cursor_token_signer=CursorTokenSigner(secret=secrets.token_bytes(32)),
        )

        async with _held_by_lifespan(a, b):
            assert _token().startswith("~")

    @pytest.mark.asyncio
    async def test_each_scope_task_keeps_its_own_signer(self) -> None:
        from forze.application.contracts.querying import (
            CursorTokenSigner,
            current_cursor_signer,
        )

        signer = CursorTokenSigner(secret=secrets.token_bytes(32))
        other = CursorTokenSigner(secret=secrets.token_bytes(32))
        seen: dict[str, object] = {}

        async with _held_by_lifespan(ExecutionRuntime(cursor_token_signer=signer)):
            async with ExecutionRuntime().scope():
                seen["unsigned"] = current_cursor_signer()

            async with ExecutionRuntime(cursor_token_signer=other).scope():
                seen["other"] = current_cursor_signer()

        assert seen == {"unsigned": None, "other": other}

    @pytest.mark.asyncio
    async def test_a_signerless_scope_defers_to_a_signer_bound_around_it(self) -> None:
        from forze.application.contracts.querying import (
            CursorTokenSigner,
            bind_cursor_signer,
            current_cursor_signer,
        )

        signer = CursorTokenSigner(secret=secrets.token_bytes(32))

        with bind_cursor_signer(signer):
            async with ExecutionRuntime().scope():
                assert current_cursor_signer() is signer
