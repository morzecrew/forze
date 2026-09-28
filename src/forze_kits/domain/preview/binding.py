"""Preview binding: a confirming command refuses when what it confirms is no longer what was shown.

A preview returns its projection with a fingerprint; the confirming command carries the
fingerprint back, and inside its transaction the projection is computed again and compared.
A mismatch refuses — never merges, never overwrites — and says the preview is stale, not what
changed: the caller may no longer be allowed to see the new value. This **detects** change
between preview and confirmation; it does not prevent it (nothing is held in between).
"""

import hashlib
import json
from collections.abc import Awaitable, Callable, Iterable, Mapping
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from ipaddress import IPv4Address, IPv6Address
from pathlib import PurePath
from typing import Any, Final, final
from uuid import UUID

import attrs
from pydantic import BaseModel, Field
from pydantic_core import Url

from forze.application.contracts.execution import Middleware, MiddlewareStep
from forze.application.contracts.transaction import IsolationLevel
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.registry.binder import OperationRegistryBinder
from forze.base.exceptions import exc
from forze.base.primitives import StrKey
from forze.domain.models import BaseDTO, CoreModel

# ----------------------- #

PREVIEW_CHANGED: Final[str] = "preview_changed"
"""Code on the refusal: what the command confirms is no longer what was previewed."""

FINGERPRINT_PREFIX: Final[str] = "sha256-c1"
"""The canonical form's version. A change to the form takes a new prefix, so a fingerprint made
under an old form refuses rather than matching."""

_STRINGABLE: Final = (PurePath, IPv4Address, IPv6Address, Url)
"""Leaf types whose ``str`` is their canonical text."""


def _leaf(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    if isinstance(value, Enum):
        return _canonical(value.value)

    if isinstance(value, (datetime, date, time)):
        return value.isoformat()

    if isinstance(value, timedelta):
        return value.total_seconds()

    if isinstance(value, (UUID, Decimal, *_STRINGABLE)):
        return str(value)

    # ``str`` of an arbitrary object may carry a memory address — stable in one process and
    # different in the next, which is the one property a fingerprint cannot lose.
    raise exc.configuration(
        f"A {type(value).__name__} in a preview projection has no canonical form; render it "
        "as a string or a number in the projection model.",
        code="preview_projection_unrenderable",
    )


def _canonical(value: Any) -> Any:
    if isinstance(value, Mapping):
        # As sorted [key, value] pairs rather than a JSON object, whose keys are text: a key
        # keeps its type ({1: …} and {"1": …} differ), and two keys that render alike (a UUID
        # and its string) are refused rather than one silently replacing the other.
        pairs = [[_canonical(key), _canonical(item)] for key, item in value.items()]

        if len({_text(key) for key, _ in pairs}) != len(pairs):
            raise exc.configuration(
                "Two keys of a mapping in a preview projection render alike; give them "
                "distinct values or one key type.",
                code="preview_projection_unrenderable",
            )

        return sorted(pairs, key=lambda pair: _text(pair[0]))

    if isinstance(value, (set, frozenset)):
        # A set's iteration order is hash-seeded per process for strings: sorted by each
        # element's own canonical text, the same set renders identically on every replica.
        return sorted((_canonical(item) for item in value), key=_text)

    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]

    return _leaf(value)


def _text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def canonical_fingerprint(model: BaseModel, *, exclude: Iterable[str] = ()) -> str:
    """The fingerprint of *model* without the *exclude* fields, as ``"sha256-c1:<hex>"``.

    The canonical form is this function's own, not a serializer's: the Python-mode dump, every
    mapping as sorted key-value pairs (a key keeps its type), every set sorted, dates and times in ISO 8601, UUIDs and decimals as strings, enums as their
    values, then standard-library JSON with sorted keys and fixed separators. It is the same in
    every process and does not move with a dependency upgrade, so a stored fingerprint stays
    recomputable.

    :raises CoreException: ``configuration`` when *exclude* names a field the model does not
        declare — an exclusion that names nothing leaves the field in — or a value has no
        canonical form.
    """

    excluded = frozenset(exclude)

    if unknown := sorted(excluded - set(type(model).model_fields)):
        raise exc.configuration(
            f"{type(model).__name__} has no field {unknown} to exclude from its fingerprint.",
            code="preview_exclusion_unknown",
        )

    payload = _canonical(model.model_dump(mode="python", exclude=set(excluded)))
    digest = hashlib.sha256(_text(payload).encode("utf-8")).hexdigest()

    return f"{FINGERPRINT_PREFIX}:{digest}"


# ....................... #


class Reviewed[T: BaseModel](BaseDTO):
    """A preview: the projection the caller is shown, and the fingerprint they confirm with."""

    data: T
    fingerprint: str


class ReviewedCommand(CoreModel):
    """The fingerprint a confirming command carries back from its preview."""

    fingerprint: str = Field(frozen=True)


type PreviewProjector[A, T: BaseModel] = Callable[[ExecutionContext, A], Awaitable[T]]
"""The application's projection: what the caller is shown for *args*. Called once for the
preview and again, inside the transaction, to check the confirmation."""


# ....................... #


def _field_names(value: Iterable[str]) -> frozenset[str]:
    # A bare string is an iterable of its letters: `exclude="hint"` would exclude "h", "i", …
    if isinstance(value, str):
        raise exc.configuration(
            f"PreviewBinding.exclude takes a collection of field names, not the string {value!r}.",
            code="preview_exclusion_unknown",
        )

    return frozenset(value)


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PreviewBinding[A, T: BaseModel]:
    """One preview-and-confirm flow, declared once and read by both sides.

    The preview operation returns :meth:`reviewed`; the confirming operation is bound with
    :meth:`bind`, and its command is the preview's arguments plus :class:`ReviewedCommand`.
    """

    name: str
    """The flow's name, which a refusal names."""

    projector: PreviewProjector[A, T]
    """What the caller is shown, computed from the operation's arguments."""

    exclude: frozenset[str] = attrs.field(factory=frozenset, converter=_field_names)
    """Projection fields the fingerprint ignores — a rendering hint or a ``generated_at`` stamp,
    never data the caller is confirming."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.name.strip():
            raise exc.configuration("PreviewBinding needs a non-blank name.")

    # ....................... #

    async def reviewed(self, ctx: ExecutionContext, args: A) -> Reviewed[T]:
        """The preview: the projection and its fingerprint."""

        data = await self.projector(ctx, args)

        return Reviewed[T](data=data, fingerprint=canonical_fingerprint(data, exclude=self.exclude))

    # ....................... #

    def bind(
        self,
        binder: OperationRegistryBinder,
        *,
        isolation: IsolationLevel = IsolationLevel.SNAPSHOT,
        step_id: StrKey | None = None,
    ) -> OperationRegistryBinder:
        """Bind the check to the confirming operations *binder* selects.

        The check is a wrap inside the operation's transaction, and the transaction runs at
        *isolation* — snapshot by default, serializable if asked. Below snapshot a write landing
        between the check and the handler's own reads is visible to the handler, so it would act
        on a state the caller never saw; read committed is therefore refused. The operations
        need a transaction route, and a backend that cannot give the level fails when they
        resolve.
        """

        if isolation < IsolationLevel.SNAPSHOT:
            raise exc.configuration(
                f"Preview {self.name!r} needs snapshot isolation or stronger: at "
                f"{isolation.name} the handler can read a state committed after the check.",
                code="preview_isolation_too_weak",
            )

        step = MiddlewareStep(
            id=step_id if step_id is not None else f"preview.{self.name}",
            factory=_ConfirmsPreview(binding=self),
        )

        return binder.bind_tx().set_isolation(isolation).wrap(step).finish()


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _ConfirmsPreview:
    binding: PreviewBinding[Any, Any]

    def __call__(self, ctx: ExecutionContext) -> Middleware[Any, Any]:
        binding = self.binding

        async def _wrap(next: Callable[[Any], Awaitable[Any]], args: Any) -> Any:
            confirmed = getattr(args, "fingerprint", None)

            if not isinstance(confirmed, str):
                raise exc.configuration(
                    f"The operation confirming preview {binding.name!r} takes a command "
                    "without a fingerprint; its command must be a ReviewedCommand.",
                    code="preview_command_unbound",
                )

            current = canonical_fingerprint(
                await binding.projector(ctx, args), exclude=binding.exclude
            )

            if current != confirmed:
                raise exc.precondition(
                    f"The preview of {binding.name!r} has changed since it was shown; show it "
                    "again and confirm what it shows now.",
                    code=PREVIEW_CHANGED,
                )

            return await next(args)

        return _wrap
