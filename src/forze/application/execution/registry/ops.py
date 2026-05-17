"""Operation namespaces and absolute references for registry-driven execution."""

from __future__ import annotations

import attrs

from forze.application.contracts.base.specs import BaseSpec
from forze.base.errors import CoreError
from forze.base.primitives import StrKey

from ..usecase import Usecase

# ----------------------- #


def _normalize_prefix(prefix: StrKey) -> str:
    raw = str(prefix)
    cleaned = raw.strip().strip(".")

    if not cleaned:
        raise CoreError("Operation namespace prefix must be non-empty")

    if ".." in cleaned or cleaned.startswith(".") or cleaned.endswith("."):
        raise CoreError(f"Invalid operation namespace prefix: {raw!r}")

    return cleaned


def _normalize_suffix(suffix: StrKey) -> str:
    raw = str(suffix)

    if not raw:
        raise CoreError("Operation suffix must be non-empty")

    if "." in raw:
        raise CoreError(
            f"Operation suffix must not contain '.', got {raw!r} "
            "(pass only the suffix, not a full operation key)",
        )

    return raw


def _normalize_key(key: StrKey) -> str:
    raw = str(key)

    if not raw:
        raise CoreError("Operation key must be non-empty")

    return raw


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class OperationNamespace:
    """Stable namespace for operation keys."""

    prefix: StrKey = attrs.field(
        converter=_normalize_prefix,
        validator=attrs.validators.instance_of(str),
    )

    # ....................... #

    def key(self, suffix: StrKey) -> str:
        return f"{self.prefix}.{_normalize_suffix(suffix)}"

    def op(self, suffix: StrKey) -> str:
        return self.key(suffix)

    def __repr__(self) -> str:
        return f"OperationNamespace(prefix={self.prefix!r})"


# ....................... #


def operation_namespace_for(spec: BaseSpec) -> OperationNamespace:
    """Return a namespace using the spec's logical name."""

    return OperationNamespace(prefix=str(spec.name))


# ....................... #


@attrs.define(slots=True, frozen=True)
class OperationRef[Args, R]:
    """Absolute operation reference used for endpoint metadata and registry APIs."""

    op: str = attrs.field(converter=_normalize_key)
    uc: type[Usecase[Args, R]] | None = attrs.field(default=None, kw_only=True)
    name: str | None = attrs.field(default=None, kw_only=True)
