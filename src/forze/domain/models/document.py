"""Domain document models and commands."""

from copy import deepcopy
from datetime import datetime
from typing import Any, ClassVar, Literal, Self, cast
from uuid import UUID

from pydantic import Field, model_validator

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, utcnow, uuid7
from forze.base.serialization import (
    apply_dict_patch,
    calculate_dict_difference,
    has_hybrid_patch_conflict,
    split_touches_from_merge_patch,
)

from .._logger import logger
from ..constants import LAST_UPDATE_AT_FIELD
from ..validation import (
    UpdateValidator,
    UpdateValidatorMetadata,
    collect_invariants,
    collect_update_validators,
)
from .aggregate import AggregateRoot
from .base import BaseDTO, CoreModel
from .emitters import has_event_emitters

# ----------------------- #


class Document(CoreModel):
    """Base document model with revision tracking and update validation."""

    _update_validators_: ClassVar[list[tuple[str, UpdateValidatorMetadata]]] = []
    """Update validators."""

    _update_validators_on_conflict: ClassVar[Literal["warn", "error", "overwrite"]] = (
        "warn"
    )
    """Update validators on conflict."""

    _invariants_: ClassVar[list[str]] = []
    """Names of ``@invariant`` methods, enforced on create and after every update."""

    # ....................... #

    id: UUID = Field(default_factory=uuid7, frozen=True)
    """Unique identifier of the document."""

    rev: int = Field(default=1, frozen=True)
    """Revision number of the document."""

    created_at: datetime = Field(default_factory=utcnow, frozen=True)
    """Timestamp of the document creation."""

    last_update_at: datetime = Field(default_factory=utcnow)
    """Timestamp of the last document update."""

    # ....................... #

    def __init_subclass__(cls, **kwargs: Any):
        super().__init_subclass__(**kwargs)

        logger.trace(
            "Collecting update validators for document subclass %s",
            cls.__qualname__,
        )

        cls._update_validators_ = collect_update_validators(
            cls,
            on_conflict=cls._update_validators_on_conflict,
        )

        logger.trace(
            "Collected %s validator(s) for %s",
            len(cls._update_validators_),
            cls.__qualname__,
        )

        cls._invariants_ = collect_invariants(cls)

        if has_event_emitters(cls) and not issubclass(cls, AggregateRoot):
            raise exc.configuration(
                f"{cls.__qualname__} declares @event_emitter methods but is not an "
                "AggregateRoot; domain events have no buffer on a plain Document."
            )

    # ....................... #

    def _validate_update_data(self, data: JsonDict) -> JsonDict:
        """Validate incoming update data against model fields and frozen flags."""

        logger.trace("Validating update data for %s", type(self).__qualname__)

        valid: JsonDict = {}
        fields = type(self).model_fields

        for k, v in data.items():
            if k not in fields:
                raise exc.domain(f"Field {k} is frozen and not allowed for update.")

            if fields[k].frozen:
                raise exc.domain(f"Field {k} is frozen and not allowed for update.")

            valid[k] = v

        return valid

    # ....................... #

    def _calculate_update_diff(self, data: JsonDict) -> tuple[Self, JsonDict]:
        """Merge ``data`` into self with full re-validation and return the result.

        The patch is merged into a python-mode dump of the current state and the
        merged mapping is run through ``model_validate``, so:

        * patch values are canonicalized by field validators (an ISO string for a
          datetime field becomes a real ``datetime``, partial dicts for nested model
          fields become validated nested models with sibling fields preserved);
        * the diff compares two python-mode dumps of *validated* state, so a patch
          that validates to the current value yields an **empty** diff (semantic
          no-op — no rev bump, no history, no emitters downstream).

        Computed fields are excluded from the dumps: they are derived (not
        persisted), and the candidate recomputes them, so leaving them in would
        leak phantom keys into the diff the gateways try to write as columns.

        :returns: The validated candidate instance and the minimal merge patch
            (python-mode values) that represents ``data`` applied to self.
        """

        logger.trace(
            "Calculating update diff for %s",
            type(self).__qualname__,
        )

        patch = self._validate_update_data(data)
        before = self._dump_stored_fields()
        merged = apply_dict_patch(before, patch)
        candidate = type(self).model_validate(merged)
        diff = calculate_dict_difference(before, candidate._dump_stored_fields())

        return candidate, diff

    # ....................... #

    def _dump_stored_fields(self) -> JsonDict:
        """Python-mode dump restricted to declared (non-computed) fields.

        ``exclude_computed_fields=True`` strips ``@computed_field`` keys at
        **every** nesting level (nested models, lists/dicts of models), not just
        the top level — computed values are derived, never persisted, so they
        must not leak into diffs the gateways write as columns. The explicit
        top-level filter is kept on purpose: it also drops pydantic *extras*,
        which are equally not part of the stored-field contract.
        """

        dump = self.model_dump(mode="python", exclude_computed_fields=True)
        fields = type(self).model_fields

        return {k: v for k, v in dump.items() if k in fields}

    # ....................... #

    def _materialize_update(
        self,
        candidate: Self,
        diff: JsonDict,
        *,
        last_update_at: datetime | None = None,
    ) -> Self:
        """Copy self, taking validated values of the changed fields from ``candidate``.

        Copying from ``self`` (rather than returning ``candidate``) preserves runtime
        state that does not round-trip through a dump — e.g. pending domain events on
        an :class:`AggregateRoot` (via its ``model_copy`` override) and fields excluded
        from serialization.

        Unchanged fields are shared with ``self`` — the normal
        ``model_copy(deep=False)`` contract. Changed values that are plain
        containers (dict/list) are deep-copied **individually**: pydantic
        validation passes ``Any``-typed (and untyped) dict/list values through
        unchanged, so without the copy the returned instance could alias the
        caller's patch containers. Typed fields get fresh containers from
        validation already. This replaces the old whole-model
        ``model_copy(deep=True)``, which deep-copied every unchanged field while
        ``model_copy`` inserted the update values uncopied anyway.

        When ``last_update_at`` is given it overrides the value sourced from
        ``candidate``, folding the timestamp bump into this single ``model_copy``
        instead of requiring a second copy of the candidate.
        """

        fields = type(self).model_fields
        update: dict[str, Any] = {}

        for k in diff:
            if k not in fields:
                continue

            v = getattr(candidate, k)

            if isinstance(v, (dict, list)):
                v = deepcopy(cast("dict[str, Any] | list[Any]", v))

            update[k] = v

        if last_update_at is not None:
            update[LAST_UPDATE_AT_FIELD] = last_update_at

        logger.trace(
            "Applying diff to %s (fields=%s)",
            type(self).__qualname__,
            tuple(update),
        )

        return self.model_copy(update=update)

    # ....................... #

    def _apply_update(self, diff: JsonDict) -> Self:
        if not diff:
            logger.trace(
                "No diff for %s; returning original instance", type(self).__qualname__
            )
            return self

        merged = apply_dict_patch(self._dump_stored_fields(), diff)
        candidate = type(self).model_validate(merged)

        return self._materialize_update(candidate, diff)

    # ....................... #

    def _run_update_validators(self, after: Self, diff: JsonDict) -> None:
        keys = diff.keys()
        cls = type(self)

        logger.trace("Running update validators for %s", cls.__qualname__)

        for name, meta in cls._update_validators_:
            if meta.fields is not None and keys.isdisjoint(meta.fields):
                logger.trace("Skipping validator %s (fields=%s)", name, meta.fields)
                continue

            logger.trace("Running validator %s (fields=%s)", name, meta.fields)

            method = cast(UpdateValidator[Self], getattr(cls, name))
            method(self, after, diff)

    # ....................... #

    @model_validator(mode="after")
    def _enforce_invariants_on_create(self) -> Self:
        # Runs on construction / model_validate (create). The merge-patch update path uses
        # ``model_copy``, which bypasses model validators, so update enforces invariants
        # explicitly via ``_run_invariants`` — together they make an invariant always hold.
        self._run_invariants()
        return self

    # ....................... #

    def _run_invariants(self) -> None:
        cls = type(self)

        for name in cls._invariants_:
            getattr(cls, name)(self)

    # ....................... #

    def update(self, data: JsonDict) -> tuple[Self, JsonDict]:
        """Apply a validated update and return the new document and diff.

        The method:

        * validates the requested field changes,
        * merges them into the current state and **re-validates** the result, so the
          returned instance always carries properly typed field values,
        * computes a JSON merge-style diff over canonical (python-mode, validated)
          values — a patch that validates to the current state yields an empty diff
          and returns ``self`` unchanged (no ``last_update_at`` bump),
        * bumps ``last_update_at`` when the diff is non-empty,
        * runs registered :func:`update_validator` hooks.

        Note that the returned instance still carries the **old** ``rev``:
        revision bumping is a persistence-strategy concern applied by the write
        gateway (e.g. the Postgres gateway bumps it under
        ``strategy="application"``), not by the domain update itself.
        """

        logger.trace(
            "Updating %s with keys=%s",
            type(self).__qualname__,
            tuple(data.keys()),
        )

        candidate, diff = self._calculate_update_diff(data)

        if diff:
            now = utcnow()
            diff[LAST_UPDATE_AT_FIELD] = now
            # `candidate` still carries the old timestamp; `_materialize_update`
            # overrides it from `now` directly so exactly one model_copy happens.
            after = self._materialize_update(candidate, diff, last_update_at=now)

        else:
            logger.trace("Update diff is empty; document remains unchanged")
            after = self

        self._run_update_validators(after, diff)

        # Enforce invariants on the new state. The create-time model validator is bypassed
        # by `model_copy` above, so this is what keeps invariants holding across updates.
        after._run_invariants()

        # Run domain-event emitters when `after` is an AggregateRoot (no-op otherwise);
        # events are recorded on the returned instance and drained by the caller.
        emit = getattr(after, "_emit_domain_events", None)
        if emit is not None:
            emit(self, diff)

        return after, diff

    # ....................... #

    def touch(self) -> tuple[Self, JsonDict]:
        """Update only ``last_update_at`` and return a new instance and diff."""

        logger.trace("Touching %s", type(self).__qualname__)

        diff = {LAST_UPDATE_AT_FIELD: utcnow()}
        model_copy = self.model_copy(update=diff)

        return model_copy, diff

    # ....................... #

    def validate_historical_consistency(self, old: Self, data: JsonDict) -> bool:
        """Check that applying ``data`` to ``old`` does not conflict with ``self``.

        This is used to prevent merging conflicting concurrent updates when
        reconstructing state from history.

        All three inputs are compared in the **canonical python-mode space**:
        ``old`` and ``self`` are dumped via :meth:`_dump_stored_fields`
        (python-mode, computed fields excluded) because ``data`` is a
        python-mode update mapping (gateways pass the codec's python-mode
        encoding of a validated update DTO). Mixing modes — json-mode dumps
        against python-mode updates — made a re-sent identical ``datetime`` or
        ``UUID`` look changed (``datetime != ISO string``), so a no-op resend
        of a field another writer concurrently changed raised a false
        ``historical_consistency_violation``. In canonical space such a resend
        yields no touch on that path, so only *genuinely* divergent values
        conflict.
        """

        logger.trace(
            "Validating historical consistency for %s with update keys=%s",
            type(self).__qualname__,
            tuple(data.keys()),
        )

        old_state = old._dump_stored_fields()
        self_state = self._dump_stored_fields()

        old_upd_state = apply_dict_patch(old_state, data)

        old_self_diff = calculate_dict_difference(old_state, self_state)
        old_upd_diff = calculate_dict_difference(old_state, old_upd_state)

        old_self_scalars, old_self_containers = split_touches_from_merge_patch(
            old_self_diff
        )
        old_upd_scalars, old_upd_containers = split_touches_from_merge_patch(
            old_upd_diff
        )

        has_conflict = has_hybrid_patch_conflict(
            old_self_scalars,
            old_self_containers,
            old_upd_scalars,
            old_upd_containers,
        )

        logger.trace("Historical consistency conflict=%s", has_conflict)

        return not has_conflict


# ....................... #


class CreateDocumentCmd(BaseDTO):
    """Deprecated base for create payloads — kept as an empty alias of :class:`BaseDTO`.

    Identity is no longer carried inside the create payload: ``create`` takes an optional
    ``id`` keyword, and ``ensure``/``upsert`` take ``id`` as an explicit argument. Create
    payloads are plain :class:`BaseDTO`; this subclass carries no fields and exists only so
    existing subclasses keep importing. New payloads should subclass :class:`BaseDTO`
    directly. To preserve ``created_at``/``last_update_at`` on import, mix in
    ``forze_kits``'s import-timestamps mixin onto the payload and use ``ensure``.
    """


# ....................... #


class ReadDocument(BaseDTO):
    """Read document model."""

    id: UUID
    """Unique identifier of the document."""

    rev: int
    """Revision number of the document."""

    created_at: datetime
    """Timestamp of the document creation."""

    last_update_at: datetime
    """Timestamp of the last document update."""


# ....................... #
# Document history


class DocumentHistory[D: Document](BaseDTO):
    """Document history entry representation."""

    source: str
    """Source of the document."""

    id: UUID
    """Unique identifier of the document."""

    rev: int
    """Revision number of the document."""

    created_at: datetime = Field(default_factory=utcnow)
    """Timestamp of the document history entry creation."""

    data: D
    """Document data."""
