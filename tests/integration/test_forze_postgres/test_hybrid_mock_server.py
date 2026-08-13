"""One served app, two backends: real Postgres for documents, the mock for everything else.

The most-requested mock-server shape, and the reason the fallback merge exists at
all. It is worth an integration test rather than a unit one because the claim is precisely
that the *real* adapter answers — which a mock cannot demonstrate about itself.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio

pytest.importorskip("fastapi")

from fastapi import APIRouter, FastAPI
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.search import SearchSpec
from forze.application.execution import ExecutionRuntime
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.lifespan import runtime_lifespan
from forze_fastapi.routes import attach_document_routes
from forze_kits.aggregates.document import build_document_registry
from forze_mock.server import MockApp, build_mock_server
from forze_postgres import PostgresClient, PostgresDepsModule, PostgresDocumentConfig

# ....................... #


class _Note(Document):
    title: str = ""


class _NoteCreate(CreateDocumentCmd):
    title: str = ""


class _NoteUpdate(BaseDTO):
    title: str | None = None


class _NoteRead(ReadDocument):
    title: str = ""


class _Indexed(BaseModel):
    id: str
    title: str


NOTES = DocumentSpec(
    name="notes",
    read=_NoteRead,
    write=DocumentWriteTypes(domain=_Note, create_cmd=_NoteCreate, update_cmd=_NoteUpdate),
)

# A different spec name from the document one: the mock's search index and its document
# store share a namespace, and colliding them would hide which plane answered.
NOTE_SEARCH = SearchSpec(name="notes_index", model_type=_Indexed, fields=["title"])

_REGISTRY = build_document_registry(NOTES).freeze()

SCHEMA = """
CREATE TABLE IF NOT EXISTS public.{table} (
    id             uuid PRIMARY KEY,
    rev            bigint      NOT NULL DEFAULT 1,
    created_at     timestamptz NOT NULL DEFAULT now(),
    last_update_at timestamptz NOT NULL DEFAULT now(),
    title          text        NOT NULL
)
"""


@pytest.fixture
async def notes_table(pg_client: PostgresClient) -> str:
    table = f"notes_{uuid4().hex[:8]}"

    await pg_client.execute(sql.SQL(SCHEMA.format(table=table)))

    yield table

    await pg_client.execute(
        sql.SQL("DROP TABLE IF EXISTS {t}").format(t=sql.Identifier("public", table))
    )


def _build_app(runtime: ExecutionRuntime) -> FastAPI:
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

    # Hand-written so the search plane is exercised in the same process as the document
    # routes above — the point is that one request path reaches Postgres and another
    # reaches the mock, from a single deps list.
    @app.post("/search/index")
    async def index(row: _Indexed) -> dict[str, int]:
        await runtime.get_context().search.command(NOTE_SEARCH).upsert([row])

        return {"indexed": 1}

    @app.get("/search")
    async def search(q: str) -> list[str]:
        page = await runtime.get_context().search.query(NOTE_SEARCH).search(
            q, pagination={"limit": 10}
        )

        return [hit.id for hit in page.hits]

    return app


@pytest_asyncio.fixture
async def hybrid(
    pg_client: PostgresClient,
    notes_table: str,
) -> AsyncIterator[httpx.AsyncClient]:
    """The served app, driven over ASGI from the test's own event loop.

    Deliberately not :class:`fastapi.testclient.TestClient`. That runs the application on a
    *second* event loop in a portal thread, while ``pg_client`` — and the psycopg pool inside
    it — was created on this one. Driving an async pool across two loops leaks one checked-out
    connection per request: the transaction commits and the server sees the session go idle,
    but the checkout is never handed back. The pool then empties one request at a time, and
    the first request that finds it empty waits out ``acquire_timeout`` and fails with
    ``core.concurrency`` — an HTTP 409 that reads as a genuine conflict.

    That is what made this file flake on CI. Real deployments never had the problem, because a
    real server runs the app on the same loop as its pool, which is exactly what this restores.
    """

    mock_app = MockApp(
        build_app=_build_app,
        modules=(
            PostgresDepsModule(
                client=pg_client,
                tx={"notes"},
                rw_documents={
                    "notes": PostgresDocumentConfig(
                        read=("public", notes_table),
                        write=("public", notes_table),
                        bookkeeping_strategy="application",
                    )
                },
            ),
        ),
    )
    app = build_mock_server(mock_app)

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)

        async with httpx.AsyncClient(transport=transport, base_url="http://hybrid") as client:
            yield client


# ....................... #


class TestHybridComposition:
    @pytest.mark.asyncio
    async def test_documents_go_to_postgres_and_search_stays_in_memory(
        self,
        hybrid: httpx.AsyncClient,
        pg_client: PostgresClient,
        notes_table: str,
    ) -> None:
        created = await hybrid.post("/notes", json={"title": "hybrid"})
        assert created.status_code in (200, 201), created.text
        note_id = created.json()["id"]

        # The real plane: the row is in Postgres, read with SQL rather than through a port.
        rows = await pg_client.fetch_all(
            sql.SQL("SELECT id, title FROM {t}").format(t=sql.Identifier("public", notes_table))
        )
        assert [(str(row["id"]), row["title"]) for row in rows] == [(note_id, "hybrid")]

        # The mock plane, in the same server and the same deps list.
        indexed = await hybrid.post("/search/index", json={"id": note_id, "title": "hybrid"})
        assert indexed.status_code in (200, 201)

        found = await hybrid.get("/search", params={"q": "hybrid"})
        assert found.json() == [note_id]

    @pytest.mark.asyncio
    async def test_the_mock_never_saw_the_document(self, hybrid: httpx.AsyncClient) -> None:
        # The other half of "the real one answered": if the mock had served documents, its
        # store would hold the row — and the SQL assertion above would have passed anyway
        # only because nothing else wrote there.
        await hybrid.post("/notes", json={"title": "postgres-only"})

        state = await hybrid.get("/_mock/state/documents")
        documents = state.json()["documents"]

        assert "notes" not in documents

    @pytest.mark.asyncio
    async def test_the_report_names_which_planes_the_mock_still_backs(
        self, hybrid: httpx.AsyncClient
    ) -> None:
        # Serving a hybrid is legitimate; leaving it invisible is not.
        health = await hybrid.get("/_mock/health")

        assert health.status_code == 200
        assert health.json()["mock"] is True


class TestHybridUuidRoundTrip:
    @pytest.mark.asyncio
    async def test_the_id_postgres_assigned_is_the_one_the_client_reads(
        self,
        hybrid: httpx.AsyncClient,
        pg_client: PostgresClient,
        notes_table: str,
    ) -> None:
        posted = await hybrid.post("/notes", json={"title": "identity"})
        assert posted.status_code in (200, 201), posted.text
        created = posted.json()

        fetched = await hybrid.get(f"/notes/{created['id']}")

        assert fetched.status_code == 200, (
            f"code={fetched.headers.get('x-error-code')} body={fetched.text}"
        )
        assert fetched.json()["title"] == "identity"
        assert UUID(created["id"])  # a real UUID from the real adapter, not a synthesized one
