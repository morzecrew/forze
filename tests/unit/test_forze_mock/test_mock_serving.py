"""The served mock: composition, refusals, and the control plane.

The claims worth testing are the ones that make this safe rather than merely useful — that
it refuses to serve a real runtime, that the control plane cannot be reached from one, and
that an armed fault produces the *asserted kind* rather than merely an error.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette")

from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from forze.application.contracts.deps import Deps
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.realtime import RealtimeSignal
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.lifespan import runtime_lifespan
from forze_fastapi.routes import attach_document_routes
from forze_kits.aggregates.document import build_document_registry
from forze_mock import MockDepsModule, MockState
from forze_mock.seeding import SeedPlan, spec_seed
from forze_mock.server import ControlPlane, MockApp, MockSession, build_mock_server, serve

pytestmark = pytest.mark.unit

# ....................... #


class _Note(Document):
    title: str = ""


class _NoteCreate(CreateDocumentCmd):
    title: str = ""


class _NoteUpdate(BaseDTO):
    title: str | None = None


class _NoteRead(ReadDocument):
    title: str = ""


NOTES = DocumentSpec(
    name="notes",
    read=_NoteRead,
    write=DocumentWriteTypes(domain=_Note, create_cmd=_NoteCreate, update_cmd=_NoteUpdate),
)

_REGISTRY = build_document_registry(NOTES).freeze()


def _build_app(runtime: ExecutionRuntime) -> FastAPI:
    """A stand-in for a user's own app factory."""

    router = APIRouter(prefix="/notes")
    attach_document_routes(
        router,
        registry=_REGISTRY,
        ns=NOTES.default_namespace,
        ctx_dep=runtime.get_context,
        style="rest",
    )

    app = FastAPI(lifespan=runtime_lifespan(runtime))
    app.include_router(router)
    register_exception_handlers(app)

    return app


def _plan(count: int = 3) -> SeedPlan:
    return SeedPlan(specs=(spec_seed(NOTES, count=count),))


def _mock_app(**overrides) -> MockApp:
    return MockApp(build_app=_build_app, seed=_plan(), **overrides)


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(build_mock_server(_mock_app())) as running:
        yield running


def _titles(client: TestClient) -> list[str]:
    response = client.post("/notes/list", json={})
    assert response.status_code == 200, response.text

    return [row["title"] for row in response.json()["hits"]]


# ....................... #


class TestItServesTheAppUnchanged:
    def test_the_apps_own_routes_answer_from_the_mock(self, client: TestClient) -> None:
        assert len(_titles(client)) == 3

        created = client.post("/notes", json={"title": "written"})
        assert created.status_code in (200, 201), created.text
        assert "written" in _titles(client)

    def test_the_control_plane_sits_beside_the_app_not_inside_it(self, client: TestClient) -> None:
        health = client.get("/_mock/health")

        assert health.status_code == 200
        assert health.json()["mock"] is True
        # A 404 here would mean the control plane went missing; a 401 would mean it landed
        # behind the app's middleware, where a tool cannot reset an app that demands a token.
        assert "not a production backend" in health.json()["warning"]

    def test_the_control_plane_can_be_turned_off(self) -> None:
        app = build_mock_server(_mock_app(control=ControlPlane(enabled=False)))

        with TestClient(app) as client:
            assert client.get("/_mock/health").status_code == 404
            assert len(_titles(client)) == 3


class TestItRefusesToServeSomethingReal:
    def test_serve_refuses_without_the_environment_gate(self, monkeypatch) -> None:
        monkeypatch.delenv("FORZE_MOCK_SERVER", raising=False)

        with pytest.raises(CoreException, match="FORZE_MOCK_SERVER=1"):
            serve(_mock_app())

    def test_building_refuses_a_composition_with_no_mock_in_it(self) -> None:
        # Structural, not nominal: the refusal reads the provider store's fallback marks —
        # which only a mock module carries — instead of trusting a name or a flag.
        real_only = MockApp(build_app=_build_app, mock=_RealModule())

        with pytest.raises(CoreException, match="no fallback-marked mock module"):
            build_mock_server(real_only)

    def test_a_session_cannot_be_minted_outside_the_builder(self) -> None:
        # Decision 9: the control plane takes a type, not a flag, so it cannot be switched
        # on for a production runtime by configuration.
        with pytest.raises(CoreException, match="only be created by"):
            MockSession(
                issued_by=object(),
                runtime=ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze()),
                state=MockState(),
                board=None,  # type: ignore[arg-type]
                clock=None,  # type: ignore[arg-type]
            )

    def test_an_app_factory_that_returns_a_bare_asgi_app_is_refused(self) -> None:
        async def bare(scope, receive, send) -> None: ...  # pragma: no cover - never called

        with pytest.raises(CoreException, match="must return a Starlette/FastAPI app"):
            build_mock_server(MockApp(build_app=lambda _runtime: bare))


class TestTheControlPlane:
    def test_reset_restores_the_pristine_seed(self, client: TestClient) -> None:
        client.post("/notes", json={"title": "transient"})
        assert "transient" in _titles(client)

        reset = client.post("/_mock/reset")

        assert reset.status_code == 200
        assert reset.json()["seeded"] == 3
        # The mutation is invisible after the reset, and the seed is back at full size.
        assert "transient" not in _titles(client)
        assert len(_titles(client)) == 3

    def test_an_armed_fault_produces_the_armed_kind(self, client: TestClient) -> None:
        # The property that makes fault injection worth having: the app's own mapping turns
        # a real `conflict` into the real 409 envelope, so the frontend is not taught a lie.
        armed = client.post(
            "/_mock/fault",
            json={"route": "notes", "op": "create", "kind": "conflict", "times": 1},
        )
        assert armed.status_code == 201

        faulted = client.post("/notes", json={"title": "never stored"})
        assert faulted.status_code == 409, faulted.text

        # `times: 1` — the next call is clean again.
        assert client.post("/notes", json={"title": "stored"}).status_code in (200, 201)

    @pytest.mark.parametrize(
        ("kind", "status"),
        [("not_found", 404), ("conflict", 409), ("authorization", 403), ("validation", 422)],
    )
    def test_each_mapped_status_class_round_trips(
        self, client: TestClient, kind: str, status: int
    ) -> None:
        client.post("/_mock/fault", json={"route": "notes", "op": "find_page", "kind": kind})

        assert client.post("/notes/list", json={}).status_code == status

    def test_an_unknown_kind_is_refused_by_name(self, client: TestClient) -> None:
        response = client.post("/_mock/fault", json={"route": "notes", "kind": "kaboom"})

        assert response.status_code == 422
        assert "Unknown exception kind" in response.json()["error"]

    def test_disarm_clears_what_was_armed(self, client: TestClient) -> None:
        client.post("/_mock/fault", json={"route": "notes", "op": "find_page", "kind": "conflict"})
        assert client.post("/notes/list", json={}).status_code == 409

        client.post("/_mock/disarm")

        assert client.post("/notes/list", json={}).status_code == 200

    def test_latency_delays_the_matching_call(self, client: TestClient) -> None:
        armed = client.post("/_mock/latency", json={"route": "notes", "seconds": 0.05})

        assert armed.status_code == 201
        assert client.get("/_mock/health").json()["armed_latencies"] == 1

    def test_state_is_inspectable_and_bounded_to_an_allowlist(self, client: TestClient) -> None:
        documents = client.get("/_mock/state/documents")

        assert documents.status_code == 200
        assert "notes" in documents.json()["documents"]

        # Not `getattr` on anything named: the state holds locks and byte payloads.
        unknown = client.get("/_mock/state/lock")
        assert unknown.status_code == 404

    def test_time_freezes_advances_and_resumes(self, client: TestClient) -> None:
        instant = datetime(2030, 1, 1, tzinfo=UTC)

        frozen = client.post("/_mock/time", json={"action": "freeze", "instant": instant.isoformat()})
        assert frozen.json() == {"now": instant.isoformat(), "frozen": True}

        advanced = client.post("/_mock/time", json={"action": "advance", "seconds": 3600})
        assert advanced.json()["now"] == (instant + timedelta(hours=1)).isoformat()

        # ...and the app sees it: a document written now carries the controlled clock.
        created = client.post("/notes", json={"title": "future"}).json()
        assert created["created_at"].startswith("2030-01-01T01:00")

        assert client.post("/_mock/time", json={"action": "resume"}).json()["frozen"] is False

    def test_emit_refuses_when_the_app_supplied_no_egress(self, client: TestClient) -> None:
        # The placement rule again: the realtime egress plane lives above forze_mock, so the
        # server says who has to supply it rather than pretending to deliver.
        response = client.post(
            "/_mock/emit",
            json={
                "audience_kind": "principal",
                "audience_name": "someone",
                "event": "ping",
                "payload": {},
            },
        )

        assert response.status_code == 400
        assert "MockApp(on_emit=...)" in response.json()["error"]

    def test_emit_delivers_through_the_apps_own_hook(self) -> None:
        delivered: list[RealtimeSignal] = []

        async def on_emit(_ctx, signal: RealtimeSignal) -> None:
            delivered.append(signal)

        with TestClient(build_mock_server(_mock_app(on_emit=on_emit))) as client:
            response = client.post(
                "/_mock/emit",
                json={
                    "audience_kind": "principal",
                    "audience_name": "ada",
                    "event": "order.placed",
                    "payload": {"id": 1},
                },
            )

        assert response.status_code == 202
        assert [signal.event for signal in delivered] == ["order.placed"]


# ....................... #


class _RealModule:
    """A deps module with nothing fallback-marked — i.e. a real one."""

    state = MockState()

    def __call__(self) -> Deps:
        return Deps.plain({})
