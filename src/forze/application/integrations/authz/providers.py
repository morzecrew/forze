"""Running permission providers: one implementation for every authz plane that decides with them."""

import asyncio
from collections.abc import Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Final
from uuid import UUID

from forze.application._logger import logger
from forze.application.contracts.authz import (
    DerivedPermissionRef,
    DerivedPermissions,
    PermissionProvider,
)
from forze.base.exceptions import exc

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

DEFAULT_PROVIDER_TIMEOUT: Final = timedelta(seconds=2)
"""How long one provider's ``derive`` may take before it counts as failed."""


def _denials(provider: PermissionProvider, keys: frozenset[str]) -> set[DerivedPermissionRef]:
    return {
        DerivedPermissionRef(permission_key=key, provider=provider.name, denied=True)
        for key in keys
    }


async def derive_permissions(
    providers: Sequence[PermissionProvider],
    principal_id: UUID,
    ctx: "ExecutionContext | None",
    *,
    timeout: timedelta | None = DEFAULT_PROVIDER_TIMEOUT,
) -> frozenset[DerivedPermissionRef]:
    """Run *providers* for *principal_id* and collect what they derived.

    A provider that raises, misses *timeout*, returns something other than
    :class:`DerivedPermissions`, or names a key outside its declaration denies every key it
    declares: an outage, a hang or a typo fails closed, and a denial masks catalog bindings, so
    none becomes an authorization bypass. Every decision runs every provider, so an unbounded
    one would stall authorization for actions it does not even declare.
    """

    if not providers:
        return frozenset()

    if ctx is None:
        raise exc.internal("Permission providers need an execution context to derive from.")

    seconds = timeout.total_seconds() if timeout is not None else None
    derived: set[DerivedPermissionRef] = set()

    for provider in providers:
        try:
            async with asyncio.timeout(seconds):
                result = await provider.derive(principal_id, ctx)

            if not isinstance(result, DerivedPermissions):  # pyright: ignore[reportUnnecessaryIsInstance]
                raise TypeError(f"derive returned {type(result).__name__}")

            stray = (result.granted | result.denied) - provider.keys

        except Exception as error:
            logger.error(
                "authz.permission_provider_failed",
                provider=provider.name,
                error=type(error).__name__,
            )
            derived |= _denials(provider, provider.keys)
            continue

        if stray:
            # A result naming a key the provider never declared cannot be read as meant: a
            # misspelt denial would deny the misspelling and leave the real key to the catalog.
            # Only the declared keys are denied — a provider never reaches past its declaration,
            # so a stray key cannot revoke a permission another binding grants.
            logger.warning(
                "authz.permission_provider_undeclared_keys",
                provider=provider.name,
                keys=sorted(stray),
            )
            derived |= _denials(provider, provider.keys)
            continue

        derived |= _denials(provider, result.denied)
        derived |= {
            DerivedPermissionRef(permission_key=key, provider=provider.name)
            for key in result.granted
        }

    return frozenset(derived)
