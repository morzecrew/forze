"""Governed dynamic-read port definition.

The plane for statements the framework **cannot inspect**: the text arrives as data from a
catalog row, a semantic-layer compiler, or a generator. Everything the other read planes get
from having the statement at wiring time — a tenancy predicate reviewed once, a known output
shape, a registered query key — is unavailable here, so the port takes those guarantees over
instead of asking the caller for them:

- **read-only is engine-enforced** — the statement runs inside a ``READ ONLY`` transaction, so
  a write or DDL is refused by the database, not by a parser the framework maintains;
- **tenancy is container confinement** — a per-tenant schema (namespace tier) or per-tenant
  credentials (dedicated tier), because a statement the framework cannot read cannot be
  trusted to carry a predicate that scopes it;
- **limits ship on** — a statement byte cap, a row cap that raises rather than truncates, and
  a statement timeout, all with real defaults.

This is *not* the raw client. The raw client stays the right tool for the write half of the
same story (runtime DDL, bulk loads) under the documented "you own validation, tenancy,
portability" policy; the read half is the hot path — every dashboard render — and it is
governed here. Statements must never come from an HTTP request body: there is no route
generator for this plane and never will be.
"""

from collections.abc import Awaitable, Sequence
from typing import Protocol, TypeVar, runtime_checkable

from pydantic import BaseModel

from forze.base.primitives import JsonDict

from .specs import DynamicReadSpec
from .types import DynamicReadOptions

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #


@runtime_checkable
class BaseDynamicReadPort(Protocol):
    """Shared ``spec`` binding for dynamic-read adapters."""

    spec: DynamicReadSpec
    """``DynamicReadSpec`` for this port instance."""


# ....................... #


class DynamicReadPort(BaseDynamicReadPort, Protocol):
    """Execute a runtime-authored read statement under framework governance.

    ``run`` returns mapping rows in the statement's column order — the widget-rendering shape,
    and the only honest one when the columns are chosen at runtime. ``select`` is the analytics
    ``select_run`` twin: the output type is a runtime argument, validated at the port boundary.

    **No pagination.** A read that needs to page through more than the route's ``row_cap`` rows
    is a mis-authored statement, and offset paging over runtime SQL invites exactly the
    fan-out and cost problems the caps exist to surface.
    """

    def run(
        self,
        statement: str,
        params: JsonDict | None = None,
        *,
        options: DynamicReadOptions | None = None,
    ) -> Awaitable[Sequence[JsonDict]]:
        """Execute *statement* with bound *params* and return mapping rows.

        :param statement: The runtime-authored statement. Parameters are bound by the engine,
            never formatted into the text.
        :param params: Values bound to the statement's named placeholders.
        :param options: Per-call ceilings; they clamp down against the route's, never up.
        """
        ...  # pragma: no cover

    def select(
        self,
        return_type: type[T],
        statement: str,
        params: JsonDict | None = None,
        *,
        options: DynamicReadOptions | None = None,
    ) -> Awaitable[Sequence[T]]:
        """Execute *statement* and validate every row as *return_type*.

        Typed at the call site because the row shape is only known there: the caller that
        compiled the statement is the one that knows what it selects.
        """
        ...  # pragma: no cover
