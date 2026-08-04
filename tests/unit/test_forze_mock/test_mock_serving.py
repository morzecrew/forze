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
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.realtime import RealtimeSignal
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.exceptions import CoreException
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
from forze_mock.server.control import _INSPECTABLE_STORES
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
