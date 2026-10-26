"""The machine contract of ``forze ops call``: its exit codes and its one stdout document.

Released contract: scripts and schedulers match on these codes and fields, so changing one
is a breaking change. The reference page (``reference/ops-cli.md``) mirrors this module.
"""

import json
import signal
from collections.abc import Mapping
from typing import Any, Final

from pydantic import BaseModel, TypeAdapter

from forze.application.execution.context import InvocationMetadata
from forze.application.execution.operations import OperationDescriptor
from forze.base.codecs import JsonCodec
from forze.base.exceptions import ErrorEnvelope, ExceptionKind

# ----------------------- #

EXIT_OK: Final = 0
EXIT_INTERNAL: Final = 1
EXIT_USAGE: Final = 2
EXIT_UNAVAILABLE: Final = 69
"""``EX_UNAVAILABLE``: a retryable failure after which a write may have landed."""

EXIT_TEMPFAIL: Final = 75
"""``EX_TEMPFAIL``: a retryable failure that is safe to repeat as is."""

EXIT_CONFIG: Final = 78
"""``EX_CONFIG``: a declaration, runtime or profile refused before the operation ran."""

# Non-retryable kinds only: retryability is read from the egress policy first, so a kind
# whose policy changes moves between the retry codes and this table on its own.
KIND_EXIT_CODES: Final[Mapping[ExceptionKind, int]] = {
    ExceptionKind.INTERNAL: EXIT_INTERNAL,
    ExceptionKind.NOT_FOUND: 3,
    ExceptionKind.VALIDATION: 4,
    ExceptionKind.DOMAIN: 4,
    ExceptionKind.PRECONDITION: 4,
    ExceptionKind.CONFLICT: 4,
    ExceptionKind.AUTHENTICATION: 5,
    ExceptionKind.AUTHORIZATION: 5,
    ExceptionKind.TIMEOUT: 6,
    ExceptionKind.CONFIGURATION: EXIT_CONFIG,
}

INTERRUPTED: Final = "interrupted"
"""The one error kind (and code) the CLI adds: a signal stopped the call."""

# ....................... #


def exit_code(envelope: ErrorEnvelope, *, repeatable: bool) -> int:
    """The exit code for *envelope*, a call that failed.

    :param repeatable: Repeating the call cannot apply anything twice: a query, or a
        command called with an idempotency key.
    """

    if envelope.retryable:
        return EXIT_TEMPFAIL if repeatable else EXIT_UNAVAILABLE

    return KIND_EXIT_CODES.get(envelope.kind, EXIT_INTERNAL)


# ....................... #


def interrupted_exit_code(signum: int) -> int:
    """128 plus the signal number, as a shell reports a process a signal stopped."""

    return 128 + signum


# ....................... #


def _document(error: Mapping[str, Any], metadata: InvocationMetadata) -> str:
    return json.dumps(
        {
            "error": error,
            "execution_id": str(metadata.execution_id),
            "correlation_id": str(metadata.correlation_id),
        }
    )


def error_document(envelope: ErrorEnvelope, metadata: InvocationMetadata) -> str:
    """The error object for a failed call, carrying the invocation ids."""

    return _document(
        {
            "code": envelope.code,
            "kind": str(envelope.kind),
            "detail": envelope.detail,
            "retryable": envelope.retryable,
            "context": envelope.context or {},
        },
        metadata,
    )


def interrupted_document(signum: int, metadata: InvocationMetadata) -> str:
    """The error object for a call a signal stopped before any commit started."""

    return _document(
        {
            "code": INTERRUPTED,
            "kind": INTERRUPTED,
            "detail": f"Interrupted by {signal.Signals(signum).name}",
            # An operation may have side effects outside its transaction.
            "retryable": False,
            "context": {},
        },
        metadata,
    )


# ....................... #


def result_document(descriptor: OperationDescriptor | None, result: Any, *, pretty: bool) -> str:
    """Encode *result* with the operation's declared output type, as JSON.

    Without a declared type, a model dumps itself and anything else goes through the codec,
    as agent tools project it.

    :raises TypeError: When *result* is not JSON.
    """

    indent = 2 if pretty else None

    if result is None:
        return "null"

    if descriptor is not None and descriptor.output_type is not None:
        return TypeAdapter[Any](descriptor.output_type).dump_json(result, indent=indent).decode()

    if isinstance(result, BaseModel):
        return result.model_dump_json(indent=indent)

    codec = JsonCodec()

    return json.dumps(codec.loads(codec.dumps(result)), indent=indent)
