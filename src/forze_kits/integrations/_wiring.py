"""Which configuration errors say the deployment is wired wrong, for every tenant alike."""

from typing import Final

from forze.application.contracts.tenancy import TENANT_GLOBAL_REQUIRED_CODE
from forze.application.integrations.crypto import PAYLOAD_CIPHER_MISSING_CODE
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #

WIRING_ERROR_CODES: Final[frozenset[str]] = frozenset(
    {
        TENANT_GLOBAL_REQUIRED_CODE,
        "outbox_relay_tenant_unbound",
        PAYLOAD_CIPHER_MISSING_CODE,
    }
)
"""Configuration errors that no tenant, and no retry, can clear.

The kind alone does not say that. A tenant's own key, disabled or pending deletion, is a
``configuration`` error too, and a loop serving many tenants that stopped on one would stop
every other tenant's work with it. Such a loop stops only on these codes; any other
configuration error is logged and retried on that tenant's next pass."""


def is_wiring_error(error: BaseException) -> bool:
    """Whether *error* is one of :data:`WIRING_ERROR_CODES`."""

    return (
        isinstance(error, CoreException)
        and error.kind is ExceptionKind.CONFIGURATION
        and error.code in WIRING_ERROR_CODES
    )
