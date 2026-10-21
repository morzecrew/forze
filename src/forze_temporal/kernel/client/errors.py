from forze_temporal._compat import require_temporal

require_temporal()

# ....................... #

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Final, Literal

from temporalio.api.errordetails.v1 import NamespaceNotFoundFailure
from temporalio.client import ScheduleAlreadyRunningError
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from forze.application.contracts.durable.workflow import (
    SCHEDULE_ALREADY_EXISTS_CODE,
    SCHEDULE_NOT_FOUND_CODE,
    WORKFLOW_ALREADY_EXISTS_CODE,
    WORKFLOW_NOT_FOUND_CODE,
)
from forze.base.exceptions import CoreException

# ----------------------- #

_RESOURCES: Final = {
    # A signal, update or terminate to a closed run gets the same answer as a missing id.
    "workflow": (
        "not found or already closed",
        WORKFLOW_NOT_FOUND_CODE,
        WORKFLOW_ALREADY_EXISTS_CODE,
    ),
    "schedule": ("not found", SCHEDULE_NOT_FOUND_CODE, SCHEDULE_ALREADY_EXISTS_CODE),
}
"""Per resource: the missing summary, the ``not_found`` code and the ``conflict`` code."""

# ....................... #


def _missing_namespace(error: RPCError) -> bool:
    """Whether a ``NOT_FOUND`` answer is about the namespace rather than the addressed id.

    That is a deployment fault, not the caller's, and stays unmapped. Told apart by its own
    detail: the time-skipping test server sends no ``NotFoundFailure`` for a missing id.
    """

    return any(
        detail.Is(NamespaceNotFoundFailure.DESCRIPTOR) for detail in error.grpc_status.details
    )


# ....................... #


@contextmanager
def map_id_errors(resource: Literal["workflow", "schedule"], resource_id: str) -> Iterator[None]:
    """Raise a missing or already taken *resource_id* as ``not_found`` / ``conflict``.

    Both are caused by the caller, so neither may reach an API as an unhandled error. Any
    other Temporal error propagates unchanged.

    :param resource: What the id names, used in the summary and the error code.
    :param resource_id: The server-side id the call addressed.
    """

    label = f"{resource.capitalize()} {resource_id!r}"
    missing, not_found_code, conflict_code = _RESOURCES[resource]

    try:
        yield

    except (WorkflowAlreadyStartedError, ScheduleAlreadyRunningError) as error:
        raise CoreException.conflict(f"{label} already exists", code=conflict_code) from error

    except RPCError as error:
        if error.status == RPCStatusCode.ALREADY_EXISTS:
            raise CoreException.conflict(f"{label} already exists", code=conflict_code) from error

        if error.status == RPCStatusCode.NOT_FOUND and not _missing_namespace(error):
            raise CoreException.not_found(f"{label} {missing}", code=not_found_code) from error

        raise
