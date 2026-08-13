"""The analytics ingest contract, held against a real Postgres over the ``COPY`` execution.

Switching `append` from a multi-VALUES `INSERT` to `COPY` is an execution change behind an
unchanged port, so the existing ingest suite is the regression harness and it runs unedited.
What it does *not* cover is the two column shapes the switch could plausibly break — a
field-encrypted column, whose value is envelope bytes rather than a scalar, and a jsonb
column, where text-mode `COPY` and bound parameters disagree about what a Python `dict` is.

Both are claims the design makes and neither had a test, which is the only reason this file
exists: reading the encoder and concluding "bytes are bytes" is not evidence.
"""

from __future__ import annotations

import base64
import json
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import (
    AnalyticsQueryDefinition,
    AnalyticsSpec,
    IngestSpec,
)
from forze.application.contracts.crypto import (
    AesGcmAead,
    FieldEncryption,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.integrations.analytics import resolve_analytics_codecs_spec
from forze.application.integrations.crypto import Keyring
from forze.base.crypto import is_envelope
from forze_mock import MockKeyManagement
from forze_postgres.adapters.analytics import PostgresAnalyticsAdapter
from forze_postgres.execution.deps.configs import PostgresAnalyticsConfig, PostgresQueryConfig
from forze_postgres.kernel.client import PostgresClient

pytestmark = pytest.mark.integration

# ----------------------- #


class _SealedRow(BaseModel):
    id: str
    region: str
    email: str


class _Params(BaseModel):
    pass


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _sealed_spec() -> AnalyticsSpec[_SealedRow, _SealedRow]:
    return resolve_analytics_codecs_spec(  # type: ignore[return-value]
        AnalyticsSpec(
            name="sealed_events",
            read=_SealedRow,
            queries={"all": AnalyticsQueryDefinition(params=_Params)},
            ingest=_SealedRow,
            encryption=FieldEncryption(encrypted=frozenset({"email"})),
        ),
        keyring=_keyring(),
        deterministic=None,
        tenant_provider=lambda: None,
    )


# ----------------------- #


class TestSealedColumnsThroughCopy:
    """A field-encrypted column is envelope bytes; ``COPY`` must carry them unchanged."""

    @pytest.mark.asyncio
    async def test_sealed_values_land_as_envelopes_and_read_back_plain(
        self,
        pg_client: PostgresClient,
    ) -> None:
        """Sealed at rest, plaintext through the port — the same contract as before the switch.

        The at-rest half is the one that matters here: a second read that goes around the
        codec proves the column really holds an envelope, so "it round-tripped" cannot be
        satisfied by a path that never encrypted anything.
        """

        table = f"sealed_events_{uuid4().hex[:12]}"
        await pg_client.execute(
            f"CREATE TABLE public.{table} (id text, region text, email text)"  # table name is a test-local literal, never caller input
        )

        adapter = PostgresAnalyticsAdapter(
            client=pg_client,
            spec=_sealed_spec(),
            config=PostgresAnalyticsConfig(
                queries={
                    "all": PostgresQueryConfig(
                        sql=f"SELECT id, region, email FROM public.{table}",  # table name is a test-local literal, never caller input
                    ),
                },
                ingest=IngestSpec(("public", table)),
            ),
        )

        await adapter.append(
            [
                _SealedRow(id="a", region="eu", email="alice@example.com"),
                _SealedRow(id="b", region="us", email="bob@example.com"),
            ],
        )

        # The witness: read the column with the driver, around the decrypting codec.
        at_rest = await pg_client.fetch_all(
            f"SELECT id, region, email FROM public.{table} ORDER BY id"  # table name is a test-local literal, never caller input
        )

        assert [row["region"] for row in at_rest] == ["eu", "us"], "a dimension must stay plain"

        for row in at_rest:
            # The column holds the base64 of an envelope, so the marker check has to decode
            # first — asserting on the string alone would pass for any opaque-looking value.
            assert is_envelope(base64.b64decode(row["email"], validate=True)), (
                f"email stored unsealed: {row['email']!r}"
            )
            assert "@example.com" not in row["email"]

        page = await adapter.run_page("all", _Params())
        emails = sorted(row.email for row in page.hits)

        assert emails == ["alice@example.com", "bob@example.com"]


# ....................... #


class TestJsonColumnsThroughCopy:
    """Text-mode ``COPY`` and bound parameters disagree about a Python ``dict``."""

    @pytest.mark.asyncio
    async def test_a_json_column_ingests_as_text(self, pg_client: PostgresClient) -> None:
        """The encoder hands JSON across as text, which is what text-mode ``COPY`` wants.

        Worth pinning rather than assuming: `encode_ingest_payloads` runs in ``"python"``
        mode, so a value that stayed a ``dict`` would reach `COPY` as one — and text-mode
        `COPY` rejects a mapping outright where a bound parameter would have adapted it.
        This is the check that would fail if the encoder ever stopped stringifying.
        """

        class _JsonRow(BaseModel):
            id: str
            payload: str

        table = f"json_events_{uuid4().hex[:12]}"
        await pg_client.execute(
            f"CREATE TABLE public.{table} (id text, payload jsonb)"  # table name is a test-local literal, never caller input
        )

        adapter = PostgresAnalyticsAdapter(
            client=pg_client,
            spec=AnalyticsSpec(
                name="json_events",
                read=_JsonRow,
                queries={"all": AnalyticsQueryDefinition(params=_Params)},
                ingest=_JsonRow,
            ),
            config=PostgresAnalyticsConfig(
                queries={
                    "all": PostgresQueryConfig(
                        sql=f"SELECT id, payload::text AS payload FROM public.{table}",  # table name is a test-local literal, never caller input
                    ),
                },
                ingest=IngestSpec(("public", table)),
            ),
        )

        await adapter.append([_JsonRow(id="a", payload=json.dumps({"nested": {"tab": "\t"}}))])

        stored = await pg_client.fetch_value(f"SELECT payload FROM public.{table}")  # table name is a test-local literal, never caller input

        assert stored == {"nested": {"tab": "\t"}}, "JSON text must land as a document"


# ....................... #


class TestAppendCap:
    """The cap outlived its rationale; it must still refuse."""

    @pytest.mark.asyncio
    async def test_a_batch_over_the_cap_is_still_refused(
        self,
        pg_client: PostgresClient,
        pg_analytics_table: str,
    ) -> None:
        """``COPY`` has no parameter ceiling, so the cap is now a guard rather than a limit.

        It stays because a governed route should still refuse a surprise mega-call — which
        means the refusal has to keep working after the reason for it changed.
        """

        class _Ingest(BaseModel):
            event: str
            value: int = 1

        adapter = PostgresAnalyticsAdapter(
            client=pg_client,
            spec=AnalyticsSpec(
                name="events",
                read=_Ingest,
                queries={"all": AnalyticsQueryDefinition(params=_Params)},
                ingest=_Ingest,
            ),
            config=PostgresAnalyticsConfig(
                queries={
                    "all": PostgresQueryConfig(
                        sql=f"SELECT event, value FROM public.{pg_analytics_table}",  # table name is a test-local literal, never caller input
                    ),
                },
                ingest=IngestSpec(("public", pg_analytics_table)),
                max_append_rows=10,
            ),
        )

        with pytest.raises(Exception, match="max_append_rows"):
            await adapter.append([_Ingest(event="e", value=index) for index in range(11)])

        assert await pg_client.fetch_value(
            f"SELECT count(*) FROM public.{pg_analytics_table}"  # table name is a test-local literal, never caller input
        ) == 0
