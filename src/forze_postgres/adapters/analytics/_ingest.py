"""Ingest (append) for Postgres analytics."""

from typing import Any, Sequence, TypeVar

from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsAppendResult
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_dump, pydantic_validate

from ._mixin_base import PostgresAnalyticsMixinBase

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
Ing = TypeVar("Ing", bound=BaseModel)

# ....................... #


class PostgresAnalyticsIngestMixin[R: BaseModel, Ing: BaseModel](
    PostgresAnalyticsMixinBase[R, Ing],
):
    """Batch INSERT ingest into a configured table."""

    async def append(self, rows: Sequence[Ing]) -> AnalyticsAppendResult | None:
        host = self._host

        if host.spec.ingest is None:
            raise exc.internal(
                f"Analytics ingest is not configured for route {host.spec.name!r}."
            )

        if host.config.resolved_ingest_relation() is None:
            raise exc.internal(
                f"Postgres ingest relation is required for route {host.spec.name!r}."
            )

        if not rows:
            return AnalyticsAppendResult(accepted=0)

        max_append = host._max_append_rows()  # type: ignore[protected-access]

        if len(rows) > max_append:
            raise exc.internal(
                f"Analytics append batch exceeds max_append_rows ({max_append})."
            )

        ingest_type = host.spec.ingest
        payloads: list[JsonDict] = []

        for row in rows:
            if isinstance(row, ingest_type):
                payloads.append(pydantic_dump(row))

            elif isinstance(
                row, BaseModel
            ):  # pyright: ignore[reportUnnecessaryIsInstance]
                payloads.append(
                    pydantic_dump(pydantic_validate(ingest_type, row.model_dump()))
                )

            else:
                raise exc.internal(
                    "Analytics ingest rows must be Pydantic model instances."
                )

        keys = list(payloads[0].keys())
        col_idents = [sql.Identifier(k) for k in keys]
        row_template = (
            sql.SQL("(")
            + sql.SQL(", ").join(sql.Placeholder() for _ in keys)
            + sql.SQL(")")
        )
        value_parts = [row_template] * len(payloads)
        flat_params: list[Any] = []

        for payload in payloads:
            flat_params.extend(payload[k] for k in keys)

        ingest_qn = await host._ingest_qname()  # type: ignore[protected-access]

        stmt = sql.SQL("INSERT INTO {table} ({cols}) VALUES {vals}").format(
            table=ingest_qn.ident(),
            cols=sql.SQL(", ").join(col_idents),
            vals=sql.SQL(", ").join(value_parts),
        )

        async def _run() -> None:
            await host.client.execute(stmt, flat_params)

        await host._run_with_timeout(None, _run)  # type: ignore[protected-access]

        return AnalyticsAppendResult(accepted=len(rows))
