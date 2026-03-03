from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Optional, Self, Sequence, final

import attrs
from psycopg import sql

from forze.application.contracts.document import DocumentSearchSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresQualifiedName:
    schema: Optional[str] = None
    name: str

    # ....................... #

    def ident(self) -> sql.Composable:
        if self.schema:
            return sql.SQL(".").join(
                [sql.Identifier(self.schema), sql.Identifier(self.name)]
            )

        return sql.Identifier(self.name)

    # ....................... #

    def string(self) -> str:
        if self.schema:
            return f"{self.schema}.{self.name}"

        return self.name

    # ....................... #

    def literal(self) -> sql.Composable:
        if self.schema:
            return sql.Literal(f"{self.schema}.{self.name}")

        return sql.Literal(self.name)

    # ....................... #

    @classmethod
    def from_string(cls, x: str) -> Self:
        if "." in x:
            schema, name = x.split(".", 1)
            return cls(schema=schema, name=name)

        return cls(name=x)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class PostgresTableSpec:
    """Postgres table specification."""

    table: str = attrs.field(on_setattr=attrs.setters.frozen)
    schema: Optional[str] = attrs.field(default=None, on_setattr=attrs.setters.frozen)

    # ....................... #

    def ident(self) -> sql.Composable:
        if self.schema:
            return sql.SQL(".").join(
                [sql.Identifier(self.schema), sql.Identifier(self.table)]
            )

        return sql.Identifier(self.table)

    # ....................... #

    def literal(self) -> sql.Composable:
        if self.schema:
            return sql.Literal(f"{self.schema}.{self.table}")

        return sql.Literal(self.table)

    # ....................... #

    def string(self) -> str:
        if self.schema:
            return f"{self.schema}.{self.table}"

        return self.table

    # ....................... #

    @classmethod
    def from_relation(cls, relation: str) -> Self:
        try:
            schema, table = relation.split(".")

        except ValueError:
            raise ValueError(f"Invalid relation: {relation}")

        return cls(schema=schema, table=table)


# ....................... #
#! GET RID OF THIS SPEC
#! should we add "type" field for search index? so then we can adjust ...


@final  #! Questionable as it's suitable only for pgroonga search
@attrs.define(slots=True, kw_only=True)
class PostgresSearchIndexSpec:
    """Postgres search index specification."""

    name: str = attrs.field(on_setattr=attrs.setters.frozen)
    fields: Sequence[str] = attrs.field(
        validator=attrs.validators.min_len(1),
        on_setattr=attrs.setters.frozen,
    )
    weights: list[int] = attrs.field(factory=list)
    fuzzy_max: float = attrs.field(
        default=0.34,
        validator=[attrs.validators.ge(0.0), attrs.validators.le(1.0)],
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.weights:
            self.weights = [1] * len(self.fields)

        if len(self.fields) != len(self.weights):
            raise ValueError("Fields and weights must have the same length")

        if len(self.fields) != len(set(self.fields)):
            raise ValueError("Fields must be unique")

        if any(w < 0 for w in self.weights):
            raise ValueError("Weights must be non-negative")

    # ....................... #

    def build_where(
        self,
        query: str,
        *,
        use_fuzzy: bool = False,
        overwrite_weights: Optional[Sequence[int]] = None,
        overwrite_fuzzy_max: Optional[float] = None,
    ) -> tuple[sql.Composable, list[Any]]:
        if not query:
            raise ValueError("Query is required")

        if overwrite_weights:
            if len(overwrite_weights) != len(self.fields):
                raise ValueError("Weights must have the same length as fields")

            if any(w < 0 for w in overwrite_weights):
                raise ValueError("Weights must be non-negative")

        if overwrite_fuzzy_max and not (0.0 <= overwrite_fuzzy_max <= 1.0):
            raise ValueError("Fuzzy max must be between 0.0 and 1.0")

        ratio = (
            overwrite_fuzzy_max if overwrite_fuzzy_max is not None else self.fuzzy_max
        )
        weights = list(overwrite_weights or self.weights)
        params: list[Any] = [query, self.name]

        q_ph = sql.Placeholder()
        idx_ph = sql.Placeholder()
        r_ph = sql.Placeholder()
        w_ph = sql.Placeholder()

        if len(self.fields) == 1:
            field = self.fields[0]
            text_expr = sql.SQL("coalesce({}::text, '')").format(sql.Identifier(field))

            # If fuzzy: wrap with fuzzy_max_distance_ratio as well
            if use_fuzzy:
                params.append(ratio)
                cond_sql = sql.SQL(
                    "pgroonga_condition({}::text, index_name => {}::text, fuzzy_max_distance_ratio => {}::float4)"
                ).format(q_ph, idx_ph, r_ph)

            else:
                cond_sql = sql.SQL(
                    "pgroonga_condition({}::text, index_name => {}::text)"
                ).format(q_ph, idx_ph)

            return sql.SQL("{} &@~ {}").format(text_expr, cond_sql), params

        # (ARRAY[...]) expression (must match index expression)
        array_expr = sql.SQL("(ARRAY[{}])").format(
            sql.SQL(", ").join(
                sql.SQL("coalesce({}::text, '')").format(sql.Identifier(f))
                for f in self.fields
            )
        )

        params.append(weights)

        # If fuzzy: wrap with fuzzy_max_distance_ratio as well
        if use_fuzzy:
            params.append(ratio)
            cond_sql = sql.SQL(
                "pgroonga_condition({}::text, index_name => {}::text, weights => {}::int[], fuzzy_max_distance_ratio => {}::float4)"
            ).format(q_ph, idx_ph, w_ph, r_ph)

        else:
            cond_sql = sql.SQL(
                "pgroonga_condition({}::text, index_name => {}::text, weights => {}::int[])"
            ).format(q_ph, idx_ph, w_ph)

        return (
            sql.SQL("{} &@~ {}").format(array_expr, cond_sql),
            params,
        )

    # ....................... #

    @classmethod
    def from_dict(cls, data: DocumentSearchSpec) -> list[Self]:
        if not data:
            return []

        out: list[Self] = []

        for k, v in data.items():
            if isinstance(v, tuple):
                out.append(cls(name=k, fields=v))

            else:
                out.append(cls(name=k, fields=list(v.keys()), weights=list(v.values())))

        return out
