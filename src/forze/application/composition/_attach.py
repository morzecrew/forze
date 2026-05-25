"""Shared helpers for transport attach (protocol-agnostic)."""

from collections.abc import Sequence

#! TODO: remove this shit


def normalize_enable(
    enable: Sequence[str] | tuple[str, ...] | None,
    *,
    default: Sequence[str],
) -> set[str]:
    """Resolve ``enable`` to a set of operation names."""

    if enable is None:
        return set(default)

    return set(enable)
