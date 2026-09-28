"""The audit spec: which action is audited, and the metadata it may record."""

import math
from collections.abc import Iterable, Mapping
from typing import Final, Literal, final, get_args
from uuid import UUID

import attrs

from forze.base.exceptions import exc
from forze.base.scrubbing.policy import is_sensitive_key

from .value_objects import AuditScalar

# ----------------------- #

AUDIT_DECLARATION: Final[str] = "audit_declaration"
"""Code on a refused :class:`AuditSpec` declaration."""

AUDIT_METADATA_REFUSED: Final[str] = "audit_metadata_refused"
"""Code on metadata refused at the call: an undeclared key, or a value that is not a scalar."""

AuditReads = Literal["after_authz", "never"]
"""How a read (a ``QUERY`` operation) is audited: after it is admitted and completes, or not."""

AuditFailurePolicy = Literal["fail", "ignore"]
"""What a failed audit write does to an operation that otherwise succeeded."""


def _metadata_names(value: Iterable[str]) -> frozenset[str]:
    # A bare string is an iterable of its letters: `allowed_metadata="purpose"` would allow
    # "p", "u", "r", … and refuse the one key it was meant to name.
    if isinstance(value, str):
        raise exc.configuration(
            f"AuditSpec.allowed_metadata takes a collection of names, not the string {value!r}.",
            code=AUDIT_DECLARATION,
        )

    names = frozenset(value)

    if any(not isinstance(name, str) or not name.strip() for name in names):
        raise exc.configuration(
            "AuditSpec.allowed_metadata names a blank or non-string key.",
            code=AUDIT_DECLARATION,
        )

    return names


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AuditSpec:
    """An audited action and the metadata keys it may record — an allowlist that refuses.

    Nothing outside :attr:`allowed_metadata` is recorded, and a call that tries raises rather
    than being filtered: a filter is invisible, so the over-collecting call site survives to
    be copied, while a refusal fails the test that introduced it. An audit trail assembled from
    request bodies is a second copy of the personal data it exists to protect.
    """

    action: str
    """The action's name in the trail (``"snapshot.read"``, ``"interval.correct"``)."""

    allowed_metadata: frozenset[str] = attrs.field(factory=frozenset, converter=_metadata_names)
    """The metadata keys this action may record. A name the log scrubber treats as
    secret-bearing (``password``, ``token``, ``session``…) is refused here."""

    audit_reads: AuditReads = "after_authz"
    """For a read: record it once it is admitted and completes (``"after_authz"``), or never.
    A refused or failed read is never recorded — a log of refused reads is a record of who
    wanted to see whose data."""

    on_failure: AuditFailurePolicy = "fail"
    """``"fail"``: an operation whose audit row could not be written fails, and inside a
    transaction its write rolls back. ``"ignore"``: it succeeds and a warning is logged."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not isinstance(self.action, str) or not self.action.strip():
            raise exc.configuration("AuditSpec needs a non-blank action.", code=AUDIT_DECLARATION)

        # Literal is not enforced at runtime: "Never" or "failclosed" would otherwise read as
        # the other branch without anyone choosing it.
        if self.audit_reads not in get_args(AuditReads):
            raise exc.configuration(
                f"AuditSpec {self.action!r}: audit_reads must be one of "
                f"{list(get_args(AuditReads))}, got {self.audit_reads!r}.",
                code=AUDIT_DECLARATION,
            )

        if self.on_failure not in get_args(AuditFailurePolicy):
            raise exc.configuration(
                f"AuditSpec {self.action!r}: on_failure must be one of "
                f"{list(get_args(AuditFailurePolicy))}, got {self.on_failure!r}.",
                code=AUDIT_DECLARATION,
            )

        if sensitive := sorted(name for name in self.allowed_metadata if is_sensitive_key(name)):
            raise exc.configuration(
                f"AuditSpec {self.action!r} allows metadata {sensitive}, which name secrets; "
                "an allowlist naming one is a declaration nobody meant to write.",
                code=AUDIT_DECLARATION,
            )

    # ....................... #

    def check_metadata(self, metadata: Mapping[str, object]) -> dict[str, AuditScalar]:
        """Return *metadata* as it is stored, or refuse it.

        :raises CoreException: ``configuration`` (code ``audit_metadata_refused``) for a key
            outside :attr:`allowed_metadata` or a value that is not a scalar — naming the
            keys and types, never the values.
        """

        if undeclared := sorted(set(metadata) - self.allowed_metadata):
            raise exc.configuration(
                f"Audit action {self.action!r} does not allow metadata {undeclared}; declare "
                "them in its AuditSpec.allowed_metadata, or stop recording them.",
                code=AUDIT_METADATA_REFUSED,
            )

        checked: dict[str, AuditScalar] = {}

        for key, value in metadata.items():
            if isinstance(value, UUID):
                checked[key] = str(value)

            elif (
                value is None
                or isinstance(value, (str, int, bool))
                or (isinstance(value, float) and math.isfinite(value))
            ):
                checked[key] = value

            else:
                raise exc.configuration(
                    f"Audit action {self.action!r}: metadata {key!r} is a "
                    f"{type(value).__name__}, not a scalar (str, int, finite float, bool, "
                    "UUID or None).",
                    code=AUDIT_METADATA_REFUSED,
                )

        return checked
