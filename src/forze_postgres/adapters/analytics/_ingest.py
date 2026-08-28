"""Ingest (append) for Postgres analytics."""

from collections.abc import Sequence
from typing import TypeVar

from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsAppendResult
from forze.application.integrations.analytics import encode_ingest_payloads
from forze.base.exceptions import exc

from ._mixin_base import PostgresAnalyticsMixinBase

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
Ing = TypeVar("Ing", bound=BaseModel)

# ....................... #


class PostgresAnalyticsIngestMixin[R: BaseModel, Ing: BaseModel](
    PostgresAnalyticsMixinBase[R, Ing],
):
    """Batch ingest into a configured table."""

    async def append(self, rows: Sequence[Ing]) -> AnalyticsAppendResult | None:
        host = self

        if host.spec.ingest is None:
            raise exc.internal(f"Analytics ingest is not configured for route {host.spec.name!r}.")

        if host.config.resolved_ingest_relation() is None:
            raise exc.internal(
                f"Postgres ingest relation is required for route {host.spec.name!r}."
            )

        if not rows:
            return AnalyticsAppendResult(accepted=0)

        max_append = host._max_append_rows()  # type: ignore[protected-access]

        if len(rows) > max_append:
            raise exc.internal(f"Analytics append batch exceeds max_append_rows ({max_append}).")

        ingest_codec = host.spec.resolved_ingest_codec
        if ingest_codec is None:
            raise exc.internal(
                f"Analytics ingest codec is not configured for route {host.spec.name!r}."
            )

        payloads = await encode_ingest_payloads(ingest_codec, list(rows))

        keys = list(payloads[0].keys())
        ingest_qn = await host._ingest_qname()  # type: ignore[protected-access]

        async def _run() -> None:
            # Rows stream out of `payloads` rather than being collected first: `copy_rows`
            # consumes one at a time, so a materialized list would hold a second full copy of
            # the batch for no reason — and the cap admits 100 000 rows by default.
            #
            # Built inside `_run` on purpose. A generator hoisted out of it would be consumed
            # by the first call and arrive empty at any second one, so a retry added here
            # later would copy zero rows and report success. Nothing retries today; this
            # makes it safe if something does.
            values = (tuple(payload[key] for key in keys) for payload in payloads)

            # Text format, not binary: the encoded payload carries JSON as text and sealed
            # columns as envelope bytes, and text-mode COPY lets the server cast both —
            # which is what keeps this an execution change behind an unchanged contract.
            await host.client.copy_rows(
                (ingest_qn.schema, ingest_qn.name),
                keys,
                values,
            )

        await host._run_with_timeout(None, _run)  # type: ignore[protected-access]

        return AnalyticsAppendResult(accepted=len(rows))
