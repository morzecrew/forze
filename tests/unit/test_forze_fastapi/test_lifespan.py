"""Tests for the runtime lifespan helper (forze_fastapi.runtime_lifespan)."""

from __future__ import annotations

import asyncio
import signal
from datetime import timedelta
from unittest.mock import MagicMock

import pytest

pytest.importorskip("fastapi")

import uvicorn
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import ExecutionRuntime, build_runtime
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_fastapi import lifespan as lifespan_module
from forze_fastapi import runtime_lifespan
from forze_fastapi import serving as serving_module
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.routes import attach_document_routes
from forze_kits.aggregates.document import DocumentDTOs, build_document_registry
from forze_mock import MockDepsModule

# ----------------------- #


class _Note(Document):
    title: str = ""


class _NoteCreate(BaseDTO):
    title: str = ""


class _NoteRead(ReadDocument):
    title: str


# ....................... #


def _counting_runtime() -> tuple[ExecutionRuntime, dict[str, int]]:
    counts = {"started": 0, "stopped": 0}

    async def up(_ctx) -> None:
        counts["started"] += 1

    async def down(_ctx) -> None:
        counts["stopped"] += 1

    runtime = build_runtime(
        MockDepsModule(),
        lifecycle_steps=[LifecycleStep(id="probe", startup=up, shutdown=down)],
    )
    return runtime, counts


# ----------------------- #


class TestRuntimeLifespan:
    @pytest.mark.asyncio
    async def test_enters_and_exits_scope(self) -> None:
        runtime, counts = _counting_runtime()
        lifespan = runtime_lifespan(runtime)
        app = FastAPI()

        async with lifespan(app):
            assert counts == {"started": 1, "stopped": 0}
            assert runtime.get_context() is not None

        assert counts == {"started": 1, "stopped": 1}
        with pytest.raises(CoreException, match="not set"):
            runtime.get_context()

    @pytest.mark.asyncio
    async def test_exception_during_app_lifetime_still_shuts_down(self) -> None:
        runtime, counts = _counting_runtime()
        lifespan = runtime_lifespan(runtime)

        with pytest.raises(RuntimeError, match="app blew up"):
            async with lifespan(FastAPI()):
                raise RuntimeError("app blew up")

        assert counts == {"started": 1, "stopped": 1}
        with pytest.raises(CoreException, match="not set"):
            runtime.get_context()


class TestRuntimeLifespanWithFastAPI:
    def test_testclient_fires_startup_and_shutdown(self) -> None:
        runtime, counts = _counting_runtime()
        app = FastAPI(lifespan=runtime_lifespan(runtime))

        with TestClient(app):
            assert counts == {"started": 1, "stopped": 0}

        assert counts == {"started": 1, "stopped": 1}

    def test_generated_route_works_inside_lifespan(self) -> None:
        spec = DocumentSpec(
            name="notes",
            read=_NoteRead,
            write=DocumentWriteTypes(domain=_Note, create_cmd=_NoteCreate),
        )
        registry = build_document_registry(
            spec, DocumentDTOs(read=_NoteRead, create=_NoteCreate)
        ).freeze()

        runtime = build_runtime(MockDepsModule())

        router = APIRouter(prefix="/notes")
        attach_document_routes(
            router,
            registry=registry,
            ns=spec.default_namespace,
            ctx_dep=runtime.get_context,
            style="rest",
        )

        app = FastAPI(lifespan=runtime_lifespan(runtime))
        app.include_router(router)
        register_exception_handlers(app)

        with TestClient(app) as client:
            created = client.post("/notes", json={"title": "hello"})
            assert created.status_code == 201
            note = created.json()

            fetched = client.get(f"/notes/{note['id']}")
            assert fetched.status_code == 200
            assert fetched.json()["title"] == "hello"


# ----------------------- #


class TestRuntimeLifespanBuild:
    """``runtime_lifespan(build=...)``: the runtime is built when the app starts."""

    @pytest.mark.asyncio
    async def test_builds_at_startup_holds_the_scope_and_drops_it_after(self) -> None:
        built: list[tuple[ExecutionRuntime, dict[str, int]]] = []

        def build() -> ExecutionRuntime:
            built.append(_counting_runtime())
            return built[-1][0]

        lifespan = runtime_lifespan(build=build)

        assert built == []
        assert lifespan.runtime() is None
        with pytest.raises(CoreException, match="No runtime yet"):
            lifespan.get_context()

        async with lifespan(FastAPI()):
            runtime, counts = built[0]
            assert lifespan.runtime() is runtime
            assert lifespan.get_context() is runtime.get_context()
            assert counts == {"started": 1, "stopped": 0}

        assert counts == {"started": 1, "stopped": 1}
        assert lifespan.runtime() is None
        with pytest.raises(CoreException, match="No runtime yet"):
            lifespan.get_context()

        # Entered again once torn down, it builds a fresh runtime.
        async with lifespan(FastAPI()):
            assert lifespan.runtime() is built[1][0]

        assert len(built) == 2

    @pytest.mark.asyncio
    async def test_the_startup_checks_run_before_the_build(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def failing_checks(_app: object) -> None:
            raise exc.configuration("bad wiring")

        monkeypatch.setattr(lifespan_module, "run_startup_checks", failing_checks)
        build = MagicMock()
        lifespan = runtime_lifespan(build=build)

        with pytest.raises(CoreException, match="bad wiring"):
            async with lifespan(FastAPI()):
                pytest.fail("the app started")

        build.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_build_that_raises_fails_the_startup_and_holds_nothing(self) -> None:
        attempts: list[int] = []
        runtime, counts = _counting_runtime()

        def build() -> ExecutionRuntime:
            attempts.append(1)

            if len(attempts) == 1:
                raise RuntimeError("no settings")

            return runtime

        lifespan = runtime_lifespan(build=build)

        with pytest.raises(RuntimeError, match="no settings"):
            async with lifespan(FastAPI()):
                pytest.fail("the app started")

        assert lifespan.runtime() is None

        # Not left half-held: the next startup builds.
        async with lifespan(FastAPI()):
            assert counts["started"] == 1

    def test_a_build_that_raises_fails_the_app_startup(self) -> None:
        def build() -> ExecutionRuntime:
            raise RuntimeError("no settings")

        app = FastAPI(lifespan=runtime_lifespan(build=build))

        with pytest.raises(RuntimeError, match="no settings"), TestClient(app):
            pytest.fail("the app served")

    @pytest.mark.asyncio
    async def test_a_lifecycle_startup_that_raises_drops_the_runtime(self) -> None:
        async def boom(_ctx) -> None:
            raise RuntimeError("cannot connect")

        lifespan = runtime_lifespan(
            build=lambda: build_runtime(lifecycle_steps=[LifecycleStep(id="db", startup=boom)])
        )

        with pytest.raises(RuntimeError, match="cannot connect"):
            async with lifespan(FastAPI()):
                pytest.fail("the app started")

        assert lifespan.runtime() is None

    @pytest.mark.asyncio
    async def test_a_second_entry_while_running_is_refused_without_building(self) -> None:
        build = MagicMock(side_effect=lambda: _counting_runtime()[0])
        lifespan = runtime_lifespan(build=build)

        async with lifespan(FastAPI()):
            with pytest.raises(CoreException, match="already running") as caught:
                async with lifespan(FastAPI()):
                    pytest.fail("a second runtime started")

            assert caught.value.kind is ExceptionKind.CONFIGURATION
            # The first entry is untouched.
            assert lifespan.get_context() is not None

        assert build.call_count == 1

    @pytest.mark.parametrize("both", [True, False])
    def test_exactly_one_of_runtime_and_build(self, both: bool) -> None:
        runtime = build_runtime()

        with pytest.raises(CoreException, match="not both or neither"):
            if both:
                runtime_lifespan(runtime, build=lambda: runtime)
            else:
                runtime_lifespan()

    def test_generated_routes_resolve_the_built_runtime(self) -> None:
        spec = DocumentSpec(
            name="notes",
            read=_NoteRead,
            write=DocumentWriteTypes(domain=_Note, create_cmd=_NoteCreate),
        )
        registry = build_document_registry(
            spec, DocumentDTOs(read=_NoteRead, create=_NoteCreate)
        ).freeze()
        built: list[ExecutionRuntime] = []

        def build() -> ExecutionRuntime:
            built.append(build_runtime(MockDepsModule()))
            return built[-1]

        lifespan = runtime_lifespan(build=build)
        router = APIRouter(prefix="/notes")
        attach_document_routes(
            router,
            registry=registry,
            ns=spec.default_namespace,
            ctx_dep=lifespan.get_context,
            style="rest",
        )
        app = FastAPI(lifespan=lifespan)
        app.include_router(router)
        register_exception_handlers(app)

        assert built == []

        with TestClient(app) as client:
            created = client.post("/notes", json={"title": "hello"})
            assert created.status_code == 201
            assert client.get(f"/notes/{created.json()['id']}").json()["title"] == "hello"

        assert len(built) == 1


# ....................... #


class TestRuntimeLifespanDrain:
    """``drain_budget``: the first signal drains the runtime ahead of uvicorn's exit."""

    @pytest.mark.asyncio
    async def test_a_sigterm_drains_then_tells_the_uvicorn_server_to_exit(self) -> None:
        server = uvicorn.Server(uvicorn.Config(FastAPI()))
        lifespan = runtime_lifespan(build=build_runtime, drain_budget=timedelta(seconds=1))

        with server.capture_signals():
            async with lifespan(FastAPI()):
                runtime = lifespan.runtime()
                assert runtime is not None
                assert signal.getsignal(signal.SIGTERM) != server.handle_exit
                # Unset by the server: bounded, so an open stream cannot hold the exit.
                assert server.config.timeout_graceful_shutdown == 10

                signal.raise_signal(signal.SIGTERM)

                for _ in range(200):
                    if server.should_exit:
                        break

                    await asyncio.sleep(0.01)

                assert server.should_exit
                assert not server.force_exit
                assert runtime.draining

            # Put back for uvicorn, which restores its own predecessors.
            assert signal.getsignal(signal.SIGTERM) == server.handle_exit

    @pytest.mark.asyncio
    async def test_under_serve_uvicorn_it_leaves_the_signals_to_its_server(self) -> None:
        runtime = build_runtime()
        server = serving_module._DrainingServer(  # pyright: ignore[reportPrivateUsage]
            uvicorn.Config(FastAPI()),
            runtime,
            drain_budget=timedelta(seconds=1),
            announce_delay=timedelta(0),
        )
        lifespan = runtime_lifespan(runtime, drain_budget=timedelta(seconds=1))

        with server.capture_signals():
            installed = signal.getsignal(signal.SIGTERM)

            async with lifespan(FastAPI()):
                assert signal.getsignal(signal.SIGTERM) == installed

    @pytest.mark.asyncio
    async def test_without_a_uvicorn_server_it_warns_and_installs_nothing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The forze logger is structlog-backed and writes past caplog, so it is patched.
        logger = MagicMock()
        monkeypatch.setattr(serving_module, "_logger", logger)
        before = signal.getsignal(signal.SIGTERM)
        lifespan = runtime_lifespan(build=build_runtime, drain_budget=timedelta(seconds=1))

        async with lifespan(FastAPI()):
            assert signal.getsignal(signal.SIGTERM) == before

        logger.warning.assert_called_once()
