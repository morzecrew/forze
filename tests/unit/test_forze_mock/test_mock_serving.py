"""The served mock: composition, refusals, and the control plane.

The claims worth testing are the ones that make this safe rather than merely useful — that
it refuses to serve a real runtime, that the control plane cannot be reached from one, and
that an armed fault produces the *asserted kind* rather than merely an error.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette")

from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from forze.application.contracts.authn import AuthnSpec
from forze.application.contracts.deps import Deps
from forze.application.contracts.interception import PortCall, PortSelector
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.realtime import RealtimeSignal
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.lifespan import runtime_lifespan
from forze_fastapi.middlewares import InvocationMetadataMiddleware, SecurityContextMiddleware
from forze_fastapi.routes import attach_document_routes
from forze_fastapi.security import AuthnRequirement, HeaderApiKeyAuthn
from forze_identity.authz import policy_principal_spec
from forze_identity.builtin.local import from_mapping, local_identity_deps
from forze_kits.aggregates.document import build_document_registry
from forze_mock import MockDepsModule, MockState
from forze_mock.execution.configs import MockRouteConfig
from forze_mock.seeding import SeedPlan, spec_seed
from forze_mock.server import ControlPlane, MockApp, MockSession, build_mock_server, serve
from forze_mock.server.clock import ControlledTimeSource
from forze_mock.server import control
from forze_mock.server.control import _INSPECTABLE_STORES
from forze_mock.server.faults import ArmedFault, FaultBoard
from forze_mock.server.runner import _is_loopback

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


def _stub_serving(monkeypatch) -> tuple[list[Any], list[str]]:
    """Arm ``serve`` for inspection: uvicorn stubbed, warnings recorded.

    The warnings are taken from the module's own logger rather than from captured output —
    logging configuration is process-global here, so reading stdout makes the assertion
    depend on whichever test configured structlog last.
    """

    import uvicorn

    from forze_mock.server import runner

    served: list[Any] = []
    warnings: list[str] = []

    class _Recorder:
        """Stands in for the module logger — the real one is frozen and cannot be patched."""

        def warning(self, message: str, *args: Any, **_kwargs: Any) -> None:
            warnings.append(message % args if args else message)

        def info(self, message: str, *args: Any, **_kwargs: Any) -> None:
            _ = message, args

    monkeypatch.setenv("FORZE_MOCK_SERVER", "1")
    monkeypatch.setattr(uvicorn, "run", lambda app, **kwargs: served.append((app, kwargs)))
    monkeypatch.setattr(runner, "logger", _Recorder())

    return served, warnings


def _elapsed(call: Callable[[], object]) -> float:
    """Wall-clock seconds a call took — the control plane's delays are real sleeps."""

    started = time.perf_counter()
    call()

    return time.perf_counter() - started


def _titles(client: TestClient) -> list[str]:
    response = client.post("/notes/list", json={})
    assert response.status_code == 200, response.text

    return [row["title"] for row in response.json()["hits"]]


# ....................... #
# A second app, authenticated and tenant-partitioned — for the isolation cases.

ADA, BOB = UUID(int=0xADA), UUID(int=0xB0B)
TENANT_A, TENANT_B = UUID(int=0xA1), UUID(int=0xB1)

AUTHN = AuthnSpec(name="main", enabled_methods=frozenset({"api_key"}))

_IDENTITY = from_mapping(
    {
        "api_keys": {
            "ada-key": {"principal_id": str(ADA), "tenant_id": str(TENANT_A)},
            "bob-key": {"principal_id": str(BOB), "tenant_id": str(TENANT_B)},
        }
    }
)


def _build_authenticated_app(runtime: ExecutionRuntime) -> FastAPI:
    app = _build_app(runtime)
    app.add_middleware(InvocationMetadataMiddleware, ctx_dep=runtime.get_context)
    app.add_middleware(
        SecurityContextMiddleware,
        ctx_dep=runtime.get_context,
        authn=AuthnRequirement(
            ingress=(HeaderApiKeyAuthn(authn_spec=AUTHN, header_name="X-API-Key", required=True),),
        ),
        when_multiple_credentials="first_in_order",
    )

    return app


def _tenanted_app() -> MockApp:
    return MockApp(
        build_app=_build_authenticated_app,
        # tenant_aware: the mock partitions storage and filters rows, mirroring the tenant
        # WHERE clause a real relation carries.
        mock=MockDepsModule(routes={"notes": MockRouteConfig(tenant_aware=True)}),
        deps=(local_identity_deps(_IDENTITY, authn_route=AUTHN.name, tenancy_route=AUTHN.name),),
        seed=SeedPlan(
            specs=(
                spec_seed(
                    policy_principal_spec,
                    fixtures=({"id": str(ADA), "kind": "user"}, {"id": str(BOB), "kind": "user"}),
                ),
            )
        ),
    )


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

    def test_a_control_prefix_at_the_root_is_refused(self) -> None:
        # Mounted first, so a root prefix matches every path and the served app vanishes —
        # which presents as a totally broken server rather than as a bad prefix.
        with pytest.raises(CoreException, match="swallow every route"):
            ControlPlane(prefix="/")

    def test_an_uncallable_emit_hook_is_refused_at_declaration(self) -> None:
        with pytest.raises(CoreException, match="on_emit must be callable"):
            MockApp(build_app=_build_app, on_emit="not a hook")  # type: ignore[arg-type]

    def test_an_uncallable_app_factory_is_refused_at_declaration(self) -> None:
        with pytest.raises(CoreException, match="build_app must be callable"):
            MockApp(build_app="myapp:create")  # type: ignore[arg-type]

    def test_a_relative_control_prefix_is_refused(self) -> None:
        with pytest.raises(CoreException, match="must start with '/'"):
            ControlPlane(prefix="_mock")

    def test_a_disabled_control_plane_does_not_police_its_prefix(self) -> None:
        # Nothing is mounted, so there is nothing for the prefix to be wrong about.
        assert ControlPlane(enabled=False, prefix="whatever").prefix == "whatever"

    def test_state_and_a_prebuilt_mock_module_together_are_refused(self) -> None:
        with pytest.raises(CoreException, match="not both"):
            MockApp(build_app=_build_app, state=MockState(), mock=MockDepsModule())

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

    def test_serving_warns_about_the_open_control_plane_only_off_loopback(
        self, monkeypatch
    ) -> None:
        # `serve` is otherwise untestable end to end — it binds a port — so the run itself is
        # stubbed and what is asserted is the pair of warnings and the app handed to uvicorn.
        served, warnings = _stub_serving(monkeypatch)

        serve(_mock_app(), host="0.0.0.0", port=9999)  # noqa: S104 - the case under test

        assert served, "uvicorn.run was never reached"
        assert served[0][1]["port"] == 9999
        assert any("IN-MEMORY MOCK" in message for message in warnings)
        assert any("UNAUTHENTICATED" in message for message in warnings)

        warnings.clear()
        serve(_mock_app(), host="127.0.0.1")

        assert not any("UNAUTHENTICATED" in message for message in warnings)

    def test_serving_with_the_control_plane_off_says_so_and_stays_quiet(self, monkeypatch) -> None:
        _, warnings = _stub_serving(monkeypatch)

        serve(
            MockApp(build_app=_build_app, control=ControlPlane(enabled=False)),
            host="0.0.0.0",  # noqa: S104 - off-loopback, but nothing is exposed
        )

        assert any("control plane=disabled" in message for message in warnings)
        assert not any("UNAUTHENTICATED" in message for message in warnings)

    @pytest.mark.parametrize(
        ("host", "loopback"),
        [
            ("127.0.0.1", True),
            ("localhost", True),
            ("::1", True),
            ("0.0.0.0", False),  # noqa: S104 - the case the warning exists for
            ("10.0.0.5", False),
            ("mock.internal", False),
        ],
    )
    def test_a_bind_beyond_this_machine_is_recognised_as_one(self, host: str, loopback) -> None:
        # The control plane is deliberately unauthenticated, so which binds are loopback is
        # the whole basis of the extra warning — including "a name I cannot resolve is not".
        assert _is_loopback(host) is loopback

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
        # Timed, not merely armed: asserting the arm succeeded would pass just as happily
        # against an interceptor that ignored the board, which is most of the point of it.
        baseline = _elapsed(lambda: client.post("/notes/list", json={}))

        armed = client.post("/_mock/latency", json={"route": "notes", "seconds": 0.25})
        assert armed.status_code == 201
        assert client.get("/_mock/health").json()["armed_latencies"] == 1

        delayed = _elapsed(lambda: client.post("/notes/list", json={}))

        # A sleep is a floor, so this is a threshold rather than a race.
        assert delayed >= 0.2, f"the armed delay was not applied ({delayed:.3f}s)"
        assert baseline < 0.2

        client.post("/_mock/disarm")

        assert _elapsed(lambda: client.post("/notes/list", json={})) < 0.2

    def test_latency_leaves_other_routes_alone(self, client: TestClient) -> None:
        client.post("/_mock/latency", json={"route": "somewhere-else", "seconds": 0.25})

        assert _elapsed(lambda: client.post("/notes/list", json={})) < 0.2

    def test_state_is_inspectable_and_bounded_to_an_allowlist(self, client: TestClient) -> None:
        documents = client.get("/_mock/state/documents")

        assert documents.status_code == 200
        assert "notes" in documents.json()["documents"]

        # Not `getattr` on anything named: the state holds locks and byte payloads.
        unknown = client.get("/_mock/state/lock")
        assert unknown.status_code == 404

    def test_every_allowlisted_store_is_a_field_of_the_state(self, client: TestClient) -> None:
        # An allowlisted name that MockState does not carry used to answer `null`, which
        # reads as "that store is empty" — the one answer a debugging aid must not invent.
        for store in _INSPECTABLE_STORES:
            response = client.get(f"/_mock/state/{store}")

            assert response.status_code == 200, f"{store}: {response.text}"
            assert response.json()[store] is not None, f"{store} is allowlisted but absent"

    def test_time_freezes_advances_and_resumes(self, client: TestClient) -> None:
        instant = datetime(2030, 1, 1, tzinfo=UTC)

        frozen = client.post(
            "/_mock/time", json={"action": "freeze", "instant": instant.isoformat()}
        )
        assert frozen.json() == {"now": instant.isoformat(), "frozen": True}

        advanced = client.post("/_mock/time", json={"action": "advance", "seconds": 3600})
        assert advanced.json()["now"] == (instant + timedelta(hours=1)).isoformat()

        # ...and the app sees it: a document written now carries the controlled clock.
        created = client.post("/notes", json={"title": "future"}).json()
        assert created["created_at"].startswith("2030-01-01T01:00")

        assert client.post("/_mock/time", json={"action": "resume"}).json()["frozen"] is False

    def test_seeding_again_without_a_reset_explains_the_collision(
        self, client: TestClient
    ) -> None:
        # Determinism makes this the expected outcome: the same plan mints the same ids, so
        # "onto the current state" lands on the rows the first application wrote. No body at
        # all, because "seed again" is the natural request and the route tolerates one.
        again = client.post("/_mock/seed")

        assert again.status_code == 400, again.text
        assert '{"reset": true}' in again.json()["error"]
        assert len(_titles(client)) == 3, "the failed re-seed must not have half-written rows"

    def test_a_seed_failing_for_any_other_reason_is_not_dressed_up_as_a_collision(
        self, client: TestClient
    ) -> None:
        # Only a conflict gets the "send reset" advice — anything else has to reach the
        # caller as itself, or the route would explain every failure the same wrong way.
        armed = client.post(
            "/_mock/fault", json={"route": "notes", "op": "create", "kind": "throttled"}
        )
        assert armed.status_code == 201

        response = client.post("/_mock/seed", json={"reset": True})

        assert "seeded" not in response.json(), response.text
        assert "Injected by the mock control plane" in response.json()["error"]
        assert "reset" not in response.json()["error"], "a throttle is not a collision"

    @pytest.mark.parametrize("times", [None, 1, 3], ids=["persistent", "one-shot", "counted"])
    def test_an_armed_conflict_reaches_the_caller_instead_of_collision_advice(
        self, client: TestClient, times: int | None
    ) -> None:
        # The collision rewrite matches on kind, and `conflict` is a kind a developer can
        # arm — so the control plane must not answer a requested failure with advice about a
        # duplicate seed that never happened. `times: 1` is the case that breaks a check
        # based on "is anything still armed": the board consumes a one-shot fault *before*
        # the exception it raises reaches this handler, leaving the board empty.
        body: dict[str, object] = {"route": "notes", "op": "create", "kind": "conflict"}

        if times is not None:
            body["times"] = times

        client.post("/_mock/fault", json=body)

        response = client.post("/_mock/seed", json={"reset": True})

        assert "Injected by the mock control plane" in response.json()["error"]
        assert "reset" not in response.json()["error"]

    def test_a_fault_armed_elsewhere_does_not_suppress_the_collision_advice(
        self, client: TestClient
    ) -> None:
        # The other direction of the same mistake: a fault armed on an unrelated route is
        # still armed during a genuine collision, and must not withhold the explanation.
        client.post("/_mock/fault", json={"route": "somewhere-else", "kind": "conflict"})

        response = client.post("/_mock/seed")

        assert response.status_code == 400, response.text
        assert '{"reset": true}' in response.json()["error"]

    def test_seed_can_clear_first(self, client: TestClient) -> None:
        client.post("/notes", json={"title": "transient"})

        assert client.post("/_mock/seed", json={"reset": True}).json() == {"seeded": 3}
        assert "transient" not in _titles(client)
        assert len(_titles(client)) == 3

    def test_a_server_with_no_plan_resets_to_nothing_rather_than_failing(self) -> None:
        with TestClient(build_mock_server(MockApp(build_app=_build_app))) as client:
            reset = client.post("/_mock/reset")

            assert reset.json() == {"reset": True, "seeded": 0}
            assert client.get("/_mock/health").json()["seeded"] is False

    def test_a_store_that_drifted_off_the_state_is_reported_not_answered(
        self, client: TestClient, monkeypatch
    ) -> None:
        # The guard behind the `outbox`/`outbox_rows` bug: allowlisted but absent must say
        # so, because `null` reads as "the store is empty".
        monkeypatch.setattr(control, "_INSPECTABLE_STORES", ("documents", "ghost_store"))

        response = client.get("/_mock/state/ghost_store")

        assert response.status_code == 500
        assert "allowlist has drifted" in response.json()["error"]

    def test_a_store_rendering_bytes_says_how_many_rather_than_dumping_them(self) -> None:
        rendered = control._jsonable(
            {"blob": b"12345", "tags": {"a"}, "rows": [1, "two", None], "when": datetime.now(UTC)}
        )

        assert rendered["blob"] == "<5 bytes>"
        assert rendered["tags"] == ["a"]
        assert rendered["rows"] == [1, "two", None]
        assert isinstance(rendered["when"], str)

    @pytest.mark.parametrize(
        ("path", "body", "expected"),
        [
            pytest.param("/_mock/fault", {"route": "notes"}, "needs a 'kind'", id="fault-no-kind"),
            pytest.param(
                "/_mock/latency", {"seconds": -1}, "must not be negative", id="latency-negative"
            ),
            pytest.param(
                "/_mock/time", {"action": "sideways"}, "Unknown time action", id="time-action"
            ),
        ],
    )
    def test_a_missing_or_impossible_instruction_is_named(
        self, client: TestClient, path: str, body: dict[str, object], expected: str
    ) -> None:
        response = client.post(path, json=body)

        assert response.status_code == 422, response.text
        assert expected in response.json()["error"]

    def test_a_body_that_is_not_an_object_is_refused(self, client: TestClient) -> None:
        response = client.post("/_mock/fault", json=["conflict"])

        assert response.status_code == 422
        assert "must be JSON objects" in response.json()["error"]

    def test_freezing_without_an_instant_stops_the_clock_where_it_is(
        self, client: TestClient
    ) -> None:
        frozen = client.post("/_mock/time", json={"action": "freeze"})

        assert frozen.json()["frozen"] is True
        assert client.post("/_mock/time", json={"action": "freeze"}).json()["now"] == (
            frozen.json()["now"]
        )

    def test_freezing_at_a_naive_instant_reads_it_as_utc(self, client: TestClient) -> None:
        # Naive in, aware out. Storing it naive would poison every later read: `now()` would
        # return a naive datetime and the first comparison against an aware one — a TTL, an
        # expiry — raises instead of answering.
        frozen = client.post(
            "/_mock/time", json={"action": "freeze", "instant": "2030-01-01T00:00:00"}
        )

        assert frozen.json()["now"] == "2030-01-01T00:00:00+00:00"
        # ...and the app still writes, which a naive clock breaks on its first comparison.
        assert client.post("/notes", json={"title": "naive"}).status_code in (200, 201)

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


class TestAMalformedControlRequestIsRefusedNotCrashed:
    """Every way a control-plane body can be wrong has to answer 4xx.

    A 500 here says the *server* broke, which sends a frontend developer looking in exactly
    the wrong place — the control plane is the tool they debug the app with.
    """

    @pytest.mark.parametrize(
        ("path", "body"),
        [
            pytest.param("/_mock/fault", {"kind": "conflict", "times": "soon"}, id="times-text"),
            pytest.param("/_mock/fault", {"kind": "conflict", "times": 0}, id="times-zero"),
            pytest.param("/_mock/latency", {"seconds": "a while"}, id="latency-text"),
            pytest.param("/_mock/latency", {"seconds": "inf"}, id="latency-infinite"),
            pytest.param("/_mock/latency", {"seconds": 1e12}, id="latency-huge-but-finite"),
            pytest.param(
                "/_mock/time", {"action": "advance", "seconds": 1e300}, id="advance-unrepresentable"
            ),
            pytest.param("/_mock/fault", {"kind": "conflict", "times": 1.5}, id="times-fractional"),
            pytest.param("/_mock/latency", {"seconds": "nan"}, id="latency-nan"),
            pytest.param(
                "/_mock/time", {"action": "advance", "seconds": "inf"}, id="advance-infinite"
            ),
            pytest.param("/_mock/time", {"action": "advance", "seconds": -1}, id="advance-back"),
            pytest.param("/_mock/time", {"action": "advance", "seconds": {}}, id="advance-object"),
            pytest.param("/_mock/time", {"action": "freeze", "instant": "soon"}, id="instant-text"),
        ],
    )
    def test_an_unusable_value_answers_422(
        self, client: TestClient, path: str, body: dict[str, object]
    ) -> None:
        response = client.post(path, json=body)

        assert response.status_code == 422, response.text
        assert response.json()["error"]

    def test_a_literal_that_overflows_to_infinity_answers_422(self, client: TestClient) -> None:
        # `1e400` is a perfectly valid JSON number that Python parses to `inf` — so the
        # finiteness check cannot live in the string branch alone.
        response = client.post(
            "/_mock/latency",
            content=b'{"seconds": 1e400}',
            headers={"content-type": "application/json"},
        )

        assert response.status_code == 422, response.text
        assert "finite" in response.json()["error"]

    def test_a_body_that_is_not_utf8_answers_422(self, client: TestClient) -> None:
        # Decoding runs before parsing, so this never reaches the JSON error at all.
        response = client.post(
            "/_mock/fault",
            content=b'{"kind": "\xff\xfe"}',
            headers={"content-type": "application/json"},
        )

        assert response.status_code == 422, response.text
        assert "not valid JSON" in response.json()["error"]

    def test_an_overflowing_fault_count_answers_422(self, client: TestClient) -> None:
        response = client.post(
            "/_mock/fault",
            content=b'{"kind": "conflict", "times": 1e400}',
            headers={"content-type": "application/json"},
        )

        assert response.status_code == 422, response.text
        assert "whole number" in response.json()["error"]

    def test_advancing_off_the_end_of_time_answers_422_not_500(self, client: TestClient) -> None:
        client.post("/_mock/time", json={"action": "freeze", "instant": "9999-12-31T00:00:00Z"})

        response = client.post("/_mock/time", json={"action": "advance", "seconds": 86_400 * 2})

        assert response.status_code == 422, response.text
        assert "representable dates" in response.json()["error"]

    def test_a_representable_advance_is_still_allowed(self, client: TestClient) -> None:
        # The bound has to leave real clock tests alone: a decade is an ordinary TTL step.
        client.post("/_mock/time", json={"action": "freeze", "instant": "2030-01-01T00:00:00Z"})
        moved = client.post("/_mock/time", json={"action": "advance", "seconds": 86_400 * 3650})

        assert moved.status_code == 200, moved.text
        assert moved.json()["now"].startswith("2039-")

    def test_a_body_that_is_not_json_answers_422(self, client: TestClient) -> None:
        response = client.post(
            "/_mock/fault",
            content=b'{"kind": "conflict"',
            headers={"content-type": "application/json"},
        )

        assert response.status_code == 422, response.text
        assert "not valid JSON" in response.json()["error"]

    def test_an_unusable_signal_answers_422_with_the_offending_fields(self) -> None:
        async def on_emit(_ctx, _signal: RealtimeSignal) -> None:  # pragma: no cover - unreached
            raise AssertionError("an invalid signal must never reach the app")

        with TestClient(build_mock_server(_mock_app(on_emit=on_emit))) as client:
            response = client.post("/_mock/emit", json={"event": "no audience"})

        assert response.status_code == 422, response.text
        assert response.json()["details"]["errors"]

    def test_a_spent_fault_does_not_owe_one_more_firing(self) -> None:
        # `remaining` is how many firings are *left*, so zero means none. The control plane
        # refuses `times: 0`, but the board is the thing that defines the counter.
        call = PortCall(surface="document_command", route="notes", op="create")
        board = FaultBoard()
        board.arm_fault(
            ArmedFault(selector=PortSelector(), kind=ExceptionKind.CONFLICT, remaining=0)
        )

        assert board.take_fault(call) is None
        assert not board.faults, "the spent fault should have been dropped, not kept"

    def test_a_fault_armed_for_a_count_still_fires_that_many_times(
        self, client: TestClient
    ) -> None:
        # The other half of validating `times`: refusing junk must not have broken the
        # value the field exists for.
        client.post(
            "/_mock/fault",
            json={"route": "notes", "op": "find_page", "kind": "conflict", "times": 2},
        )

        statuses = [client.post("/notes/list", json={}).status_code for _ in range(3)]

        assert statuses == [409, 409, 200]


class TestTheControlledClockItself:
    """The time source's own contract, below the route that drives it."""

    def test_freezing_without_an_instant_stops_at_now(self) -> None:
        clock = ControlledTimeSource()

        stopped = clock.freeze()

        assert clock.frozen_at == stopped
        assert clock.now() == stopped

    def test_a_running_clock_advances_by_offset_and_keeps_running(self) -> None:
        clock = ControlledTimeSource()

        moved = clock.advance(timedelta(hours=1))

        assert clock.frozen_at is None
        assert clock.offset == timedelta(hours=1)
        assert moved - datetime.now(UTC) > timedelta(minutes=59)

    def test_resuming_a_clock_that_never_stopped_changes_nothing(self) -> None:
        clock = ControlledTimeSource()

        assert clock.resume() is not None
        assert clock.frozen_at is None
        assert clock.offset == timedelta()

    def test_advancing_past_the_end_of_time_is_refused_without_breaking_the_clock(self) -> None:
        # A representable step can still land outside the representable *range*. Checking the
        # destination rather than the step is also what leaves the clock usable afterwards.
        clock = ControlledTimeSource()
        clock.freeze(datetime(9999, 12, 31, tzinfo=UTC))

        with pytest.raises(CoreException, match="representable dates"):
            clock.advance(timedelta(days=2))

        assert clock.now() == datetime(9999, 12, 31, tzinfo=UTC), "a refused advance moved it"

    @pytest.mark.parametrize(
        "leave_it_running",
        [
            pytest.param(
                lambda clock: clock.advance(
                    datetime.max.replace(tzinfo=UTC) - clock.now() - timedelta(milliseconds=5)
                ),
                id="advance",
            ),
            pytest.param(
                lambda clock: (
                    clock.freeze(datetime.max.replace(tzinfo=UTC) - timedelta(milliseconds=5)),
                    clock.resume(),
                ),
                id="resume",
            ),
        ],
    )
    def test_a_running_clock_is_never_left_without_room_to_run(self, leave_it_running) -> None:
        # Checking the destination is not enough for a clock that keeps *moving*: its offset
        # is fixed while the wall clock advances, so an instant that is representable at the
        # moment of the call is not representable on the next read. And `now()` is what every
        # request calls, so the failure would land on all of them, not on the call at fault.
        clock = ControlledTimeSource()

        with pytest.raises(CoreException, match="must stay at least"):
            leave_it_running(clock)

        time.sleep(0.02)

        assert clock.now() is not None, "the clock was left in a state its next read cannot use"

    def test_the_clock_refuses_to_run_backwards(self) -> None:
        # The route answers 422 before reaching this, but the source is the contract.
        with pytest.raises(CoreException, match="only advances forward"):
            ControlledTimeSource().advance(timedelta(seconds=-1))

    def test_monotonic_keeps_elapsing_while_the_wall_clock_is_frozen(self) -> None:
        # Only the wall clock is controlled, so a frozen server still times out.
        clock = ControlledTimeSource()
        clock.freeze(datetime(2030, 1, 1, tzinfo=UTC))

        assert clock.monotonic() <= clock.monotonic()

    def test_a_fault_that_does_not_match_is_stepped_over(self) -> None:
        board = FaultBoard()
        board.arm_fault(
            ArmedFault(selector=PortSelector(route="elsewhere"), kind=ExceptionKind.CONFLICT)
        )

        assert board.take_fault(PortCall(surface=None, route="notes", op="create")) is None
        assert len(board.faults) == 1, "a non-matching fault must stay armed"


class TestPaginationIsReal:
    def test_the_cursor_loop_terminates_and_visits_every_row_once(self) -> None:
        """The claim §1 makes against schema mocks, asserted rather than assumed.

        A synthesized `next_cursor` is a random string, so the client's loop never ends.
        Here the cursor is the real one: the walk terminates on its own and yields each
        seeded document exactly once.
        """

        with TestClient(build_mock_server(MockApp(build_app=_build_app, seed=_plan(7)))) as client:
            seen: list[str] = []
            cursor: str | None = None

            for _ in range(20):  # a bound, so a non-terminating cursor fails instead of hanging
                body: dict[str, object] = {"limit": 2}

                if cursor is not None:
                    body["cursor"] = cursor

                page = client.post("/notes/list_cursor", json=body)
                assert page.status_code == 200, page.text

                data = page.json()
                seen.extend(row["id"] for row in data["hits"])
                cursor = data.get("next_cursor")

                if cursor is None:
                    break

            assert cursor is None, "the cursor loop did not terminate"
            assert len(seen) == 7
            assert len(set(seen)) == 7, "a document was visited twice"


class TestTenancy:
    def test_two_keys_on_different_tenants_see_disjoint_data(self) -> None:
        """One server, two credentials, two tenants — and neither sees the other's rows.

        This is the case that surfaced the tenancy-binding bug: the resolver is registered
        *routed* by the shipped modules, so nothing was bound and every request failed
        closed with `tenant_required`.
        """

        with TestClient(build_mock_server(_tenanted_app())) as client:
            ada = {"X-API-Key": "ada-key"}
            bob = {"X-API-Key": "bob-key"}

            assert client.post("/notes", json={"title": "ada-note"}, headers=ada).status_code in (
                200,
                201,
            )
            assert client.post("/notes", json={"title": "bob-note"}, headers=bob).status_code in (
                200,
                201,
            )

            def titles(headers: dict[str, str]) -> list[str]:
                response = client.post("/notes/list", json={}, headers=headers)
                assert response.status_code == 200, response.text

                return [row["title"] for row in response.json()["hits"]]

            assert titles(ada) == ["ada-note"]
            assert titles(bob) == ["bob-note"]

    def test_an_unauthenticated_request_binds_no_tenant_and_fails_closed(self) -> None:
        # The other half of isolation: no credential must not mean "every tenant".
        with TestClient(build_mock_server(_tenanted_app())) as client:
            assert client.post("/notes/list", json={}).status_code == 401


# ....................... #


class _RealModule:
    """A deps module with nothing fallback-marked — i.e. a real one."""

    state = MockState()

    def __call__(self) -> Deps:
        return Deps.plain({})
