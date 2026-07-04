"""Internal tree walk for :func:`~forze.base.scrubbing.sanitize`."""

from typing import Any, Mapping, Sequence

from pydantic import BaseModel, SecretStr

from .policy import (
    MAX_DEPTH_SENTINEL,
    SECRET_PLACEHOLDER,
    is_sensitive_key,
    scrub_log_string,
)

# ----------------------- #


def _scrub_string(value: str, *, text_scrub: bool) -> str:
    return scrub_log_string(value) if text_scrub else value


# ....................... #


def walk_value(
    value: Any,
    *,
    text_scrub: bool,
    depth: int,
    max_depth: int,
) -> Any:
    if depth > max_depth:
        return MAX_DEPTH_SENTINEL

    if isinstance(value, SecretStr):
        return str(value)

    if isinstance(value, BaseModel):
        from forze.base.serialization.pydantic import pydantic_dump

        return walk_mapping(
            pydantic_dump(value, mode="json"),
            text_scrub=text_scrub,
            depth=depth + 1,
            max_depth=max_depth,
        )

    if isinstance(value, Mapping):
        return walk_mapping(
            value,  # pyright: ignore[reportUnknownArgumentType]
            text_scrub=text_scrub,
            depth=depth + 1,
            max_depth=max_depth,
        )

    if isinstance(value, str):
        return _scrub_string(value, text_scrub=text_scrub)

    if isinstance(value, (bytes, bytearray, memoryview)):
        return value  # pyright: ignore[reportUnknownVariableType]

    if isinstance(value, Sequence):
        return [
            walk_value(
                item,
                text_scrub=text_scrub,
                depth=depth + 1,
                max_depth=max_depth,
            )
            for item in value  # pyright: ignore[reportUnknownVariableType]
        ]

    return value


# ....................... #


def walk_mapping(
    mapping: Mapping[str, Any],
    *,
    text_scrub: bool,
    depth: int,
    max_depth: int,
) -> dict[str, Any]:
    if depth > max_depth:
        return {MAX_DEPTH_SENTINEL: True}

    out: dict[str, Any] = {}

    for key, value in mapping.items():
        # A logging pipeline must never raise into application code: dict keys are
        # not always ``str`` (e.g. ``log.info("stats", counts={1: 2})``), and the
        # sensitive-key regex only accepts strings. Coerce for the check so a
        # non-str key is inspected by name without crashing the caller's log site.
        # A key whose ``__str__`` (or the sensitivity check) raises is masked rather
        # than propagated — masking is the safe failure mode for a scrubber, mirroring
        # ``EventDictSanitizer.__call__``.
        try:
            key_name = (
                key
                if isinstance(key, str)  # pyright: ignore[reportUnnecessaryIsInstance]
                else str(key)
            )
            sensitive = is_sensitive_key(key_name)

        except Exception:
            out[key] = SECRET_PLACEHOLDER
            continue

        if sensitive:
            out[key] = SECRET_PLACEHOLDER
            continue

        if isinstance(value, Mapping):
            out[key] = walk_mapping(
                value,  # pyright: ignore[reportUnknownArgumentType]
                text_scrub=text_scrub,
                depth=depth + 1,
                max_depth=max_depth,
            )

        elif isinstance(value, Sequence) and not isinstance(
            value, (str, bytes, bytearray, memoryview)
        ):
            out[key] = [
                walk_value(
                    item,
                    text_scrub=text_scrub,
                    depth=depth + 1,
                    max_depth=max_depth,
                )
                for item in value  # pyright: ignore[reportUnknownVariableType]
            ]

        else:
            out[key] = walk_value(
                value,
                text_scrub=text_scrub,
                depth=depth + 1,
                max_depth=max_depth,
            )

    return out
