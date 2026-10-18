"""Raising a principal's epoch on every write that deactivates its policy principal.

The eligibility read after a token verification may be skipped only because a token admitted on
its principal's current epoch implies nothing deactivated the principal since. That holds when
every write that makes a principal ineligible raises its epoch in the same transaction, whoever
makes it: the authz registry, the authn deactivation, or an application writing the document
through the document port. So the raise sits on the policy principal's command port itself.
"""

import inspect
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.authn import (
    PrincipalEpochDepKey,
    RevocationStatesDepKey,
    RevocationStatesSpec,
)
from forze.application.contracts.deps.proxy import PortProxy
from forze.application.contracts.document import DocumentCommandPort

from .revocation_states import EpochRaise, RevocationStates

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #


def _deactivates(update: object) -> bool:
    """Whether an update may leave the principal ineligible: it sets ``is_active`` to anything
    but ``True`` (``False``, or a null a bulk patch would store as it is)."""

    if isinstance(update, BaseModel):
        return (
            "is_active" in update.model_fields_set
            and getattr(update, "is_active", None) is not True
        )

    if isinstance(update, Mapping):
        return "is_active" in update and update["is_active"] is not True  # pyright: ignore[reportUnknownMemberType]

    # A payload of no known shape: assume the worst.
    return update is not None


def _updated(updates: Iterable[Any]) -> list[UUID]:
    return [item.id for item in updates if _deactivates(getattr(item, "dto", None))]


def _upserted(items: Iterable[Any]) -> list[UUID]:
    return [item.id for item in items if _deactivates(item.update)]


_PRINCIPALS: Mapping[str, Callable[[Mapping[str, Any]], list[UUID]]] = {
    "update": lambda a: [a["pk"]] if _deactivates(a["dto"]) else [],
    "update_many": lambda a: _updated(a["updates"]),
    "upsert": lambda a: [a["id"]] if _deactivates(a["update"]) else [],
    "upsert_many": lambda a: _upserted(a["items"]),
    "kill": lambda a: [a["pk"]],
    "kill_many": lambda a: list(a["pks"]),
    "delete": lambda a: [a["pk"]],
    "delete_many": lambda a: [pk for pk, _rev in a["deletes"]],
}
"""Per write, the principals it may deactivate, from its bound arguments. A removed row refuses
authentication as an inactive one does."""

_MATCHING = frozenset({"update_matching", "update_matching_strict"})
"""Bulk patches that name no rows: they are run asking for the rows back."""


# ....................... #


@attrs.define(slots=True)
class _EpochRaisingPrincipalPort(PortProxy):
    """The policy principal's command port, raising the epoch of every principal a write may
    deactivate, in that write's transaction, after the write took the principal's row."""

    raise_epoch: EpochRaise = attrs.field(kw_only=True)

    # ....................... #

    def _should_wrap(self, name: str, attr: Any) -> bool:
        del attr
        return name in _PRINCIPALS or name in _MATCHING

    def _wrap_async(self, name: str, attr: Any) -> Any:
        return self._raising(name, attr)

    def _wrap_sync(self, name: str, attr: Any) -> Any:
        # Whatever shape the backend gives the method, its writes are counted.
        return self._raising(name, attr)

    # ....................... #

    def _raising(self, name: str, method: Callable[..., Awaitable[Any]]) -> Any:
        signature = inspect.signature(method)

        async def raising(*args: Any, **kwargs: Any) -> Any:
            bound = signature.bind(*args, **kwargs)
            bound.apply_defaults()
            arguments = bound.arguments

            if name in _MATCHING:
                if not _deactivates(arguments["dto"]):
                    return await method(*args, **kwargs)

                self.raise_epoch.require_transaction(self.inner)
                rows: Sequence[Any] = await method(
                    **{**arguments, "return_new": True},
                )
                await self._raise([row.id for row in rows])

                return rows if arguments["return_new"] else len(rows)

            principals = _PRINCIPALS[name](arguments)

            if not principals:
                return await method(*args, **kwargs)

            # Before the write: outside a transaction, or in one that holds neither the
            # principal's write nor the epoch's, it would commit without its raise.
            self.raise_epoch.require_transaction(self.inner)
            result = await method(*args, **kwargs)
            await self._raise(principals)

            return result

        return raising

    async def _raise(self, principals: Iterable[UUID]) -> None:
        # In id order, so two bulk writes raise their shared principals in the same order.
        for principal_id in sorted(set(principals)):
            await self.raise_epoch(principal_id)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class PolicyPrincipalEpochHook:
    """Wrap the policy principal's command port so each write that may deactivate a principal
    raises its epoch in the write's transaction, and writes it through after commit.

    Registered under the policy principal document's name
    (:data:`~forze.application.contracts.document.DocumentCommandHookDepKey`) by the authz deps
    module whose kernel carries :class:`RevocationStates`. Such a write refuses to run outside a
    transaction, and above read committed (where the epoch store refuses the raise).
    """

    config: RevocationStates
    """The configuration the verifier reads; the eligibility skip requires the same value."""

    # ....................... #

    def __call__(
        self,
        ctx: "ExecutionContext",
        port: DocumentCommandPort[Any, Any, Any, Any],
    ) -> DocumentCommandPort[Any, Any, Any, Any]:
        raise_epoch = EpochRaise(
            config=self.config,
            states=ctx.deps.resolve_configurable(
                ctx,
                RevocationStatesDepKey,
                RevocationStatesSpec(name=self.config.route),
                route=self.config.route,
            ),
            epochs=ctx.deps.provide(PrincipalEpochDepKey)(ctx),
            in_transaction=lambda: ctx.tx_ctx.depth() > 0,
            defer=ctx.tx_ctx.run_or_defer,
        )

        return _EpochRaisingPrincipalPort(inner=port, raise_epoch=raise_epoch)  # type: ignore[return-value]
