"""Map raw psycopg failures onto the dynamic-read taxonomy.

Every refusal this plane advertises is one **Postgres** makes — a read-only transaction
rejecting a write, the extended protocol rejecting a second command, role grants rejecting a
relation, the statement timeout firing. This module is only the translation, and it works from
SQLSTATE rather than message text wherever SQLSTATE is specific enough, so it stays correct
under any server locale (``lc_messages``).

Class ``42`` (*syntax error or access rule violation*) and class ``22`` (*data exception*) are
handled wholesale rather than case by case. Both are caller-caused by definition, and on this
plane the statement *is* the caller's input — an undefined column here is a malformed request,
not a broken framework, so none of it may egress as ``internal`` (the error-code-hygiene rule).
"""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from psycopg import errors

from forze.application.integrations.dynamic_read import (
    multi_statement,
    permission_denied,
    statement_invalid,
    timed_out,
    write_refused,
)
from forze.base.exceptions import CoreException

# ----------------------- #

READ_ONLY_SQL_TRANSACTION = "25006"
QUERY_CANCELED = "57014"
INSUFFICIENT_PRIVILEGE = "42501"
INVALID_PARAMETER_VALUE = "22023"
SYNTAX_ERROR = "42601"

SYNTAX_OR_ACCESS_CLASS = "42"
DATA_EXCEPTION_CLASS = "22"

_CALLER_CAUSED_CLASSES = (SYNTAX_OR_ACCESS_CLASS, DATA_EXCEPTION_CLASS)
"""SQLSTATE classes whose every member is the caller's statement being wrong."""

_MULTI_COMMAND_MARKER = "multiple commands"
"""Postgres' wording for the extended protocol's single-command rule.

The only place this module reads message text, because the server reports it as a plain
``42601`` syntax error with no dedicated SQLSTATE — so a multi-command string and a stray comma
are indistinguishable by code. Matching the text refines the answer where the server speaks
English and degrades to ``dynamic_read_statement_invalid`` where it does not; both are
refusals, and neither ever lets the second command run.
"""

# ....................... #


def dynamic_read_error(error: BaseException, *, route: str) -> CoreException | None:
    """Translate a psycopg *error* into this plane's taxonomy, or ``None`` to defer.

    Deferring matters as much as translating. A dynamic read can fail for reasons that have
    nothing to do with the statement — a dead connection, a full pool, a server shutting down —
    and those already have mappings that callers handle as infrastructure. Only what the server
    attributes to *this statement* is claimed here.
    """

    if not isinstance(error, errors.Error):
        return None

    sqlstate = error.sqlstate

    if sqlstate is None:
        # No SQLSTATE means the driver rejected the call before the server saw it: an
        # unsupported placeholder in the text (a literal ``%`` that was not doubled), or a
        # statement that returned no result set at all on a plane whose contract is rows.
        # Both are the caller's statement, not the database's health.
        if isinstance(error, errors.ProgrammingError):
            return statement_invalid(route, detail=str(error))

        return None

    if sqlstate == READ_ONLY_SQL_TRANSACTION:
        return write_refused(route)

    if sqlstate == QUERY_CANCELED:
        return timed_out(route)

    if sqlstate == INSUFFICIENT_PRIVILEGE:
        return permission_denied(route, detail=_primary(error))

    if sqlstate == SYNTAX_ERROR and _MULTI_COMMAND_MARKER in _primary(error).lower():
        return multi_statement(route)

    if sqlstate[:2] in _CALLER_CAUSED_CLASSES:
        return statement_invalid(route, detail=_safe_detail(error, sqlstate))

    return None


# ....................... #


def _safe_detail(error: errors.Error, sqlstate: str) -> str:
    """The server's message, unless the message is a place data can hide.

    A ``validation`` error's details reach the client, so what goes in them matters on a plane
    whose rows belong to a tenant. Class ``42`` messages talk about the *statement* — a bad
    keyword, an unknown relation, a column that is not there — all of which the caller wrote
    and already has.

    Class ``22`` is different: a data exception names a **value**, and that value may have come
    out of a column rather than out of the statement (``SELECT some_text::int`` reports the
    stored text back). One row of somebody else's data in an error message is still one row of
    somebody else's data, so these report the SQLSTATE instead — enough to look the failure up,
    with nothing of the table in it.
    """

    if sqlstate.startswith(DATA_EXCEPTION_CLASS):
        return f"SQLSTATE {sqlstate}"

    return _primary(error)


# ....................... #


def _primary(error: errors.Error) -> str:
    """The server's primary message, or the exception's own text when it has none."""

    return str(error.diag.message_primary or error)
