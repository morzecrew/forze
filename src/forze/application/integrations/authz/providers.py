"""Running permission providers: one implementation for every authz plane that decides with them."""

import asyncio
from collections.abc import Iterable, Sequence
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


def check_permission_providers(providers: Iterable[PermissionProvider]) -> None:
    """Refuse a declaration nobody meant: a blank or repeated name, or keys that are not a
    non-empty set of permission-key strings."""

    names: set[str] = set()

    for provider in providers:
        if not provider.name.strip() or provider.name in names:
            raise exc.configuration(
                f"Permission provider name {provider.name!r} is blank or used twice; a derived "
                "grant is attributed to its provider by name.",
                code="authz_provider_declaration",
            )

        if not provider.keys:
            raise exc.configuration(
                f"Permission provider {provider.name!r} declares no keys; a provider that may "
                "grant or deny nothing is a declaration nobody meant.",
                code="authz_provider_declaration",
            )

        # Every decision reads the declaration and takes set differences against it: a list or
        # a bare string would fail there rather than here, and a mutable set could be widened
        # after this check, past the catalog check at boot.
        if not isinstance(provider.keys, frozenset) or not all(
            isinstance(key, str) for key in provider.keys
        ):
            raise exc.configuration(
                f"Permission provider {provider.name!r} must declare its keys as a frozenset of "
                f"permission-key strings, not {type(provider.keys).__name__}.",
                code="authz_provider_declaration",
            )

        names.add(provider.name)


def check_provider_timeout(timeout: timedelta | None) -> None:
    """Refuse a deadline no provider could meet; ``None`` removes the deadline."""

    if timeout is not None and timeout <= timedelta(0):
        raise exc.configuration(
            f"permission_provider_timeout must be positive, not {timeout}.",
            code="authz_provider_declaration",
        )


# ....................... #


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
    loop = asyncio.get_running_loop()
    derived: set[DerivedPermissionRef] = set()

    for provider in providers:
        try:
            async with asyncio.timeout(seconds) as budget:
                result = await provider.derive(principal_id, ctx)

            # The timer raises only into an await that lets the cancellation through: a provider
            # that swallows it, or blocks the loop past the deadline, still returns — late.
            deadline = budget.when()

            if budget.expired() or (deadline is not None and loop.time() >= deadline):
                raise TimeoutError

            if not isinstance(result, DerivedPermissions):  # pyright: ignore[reportUnnecessaryIsInstance]
                raise TypeError(f"derive returned {type(result).__name__}")

            stray = (result.granted | result.denied) - provider.keys

        except (Exception, asyncio.CancelledError) as error:
            # A cancellation aimed at this task is the caller's and propagates; one raised from
            # inside the provider, by an inner task it awaited, is the provider failing.
            if isinstance(error, asyncio.CancelledError):
                task = asyncio.current_task()

                if task is None or task.cancelling():
                    raise

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
