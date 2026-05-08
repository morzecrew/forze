from typing import Any, Sequence

from .specs import SearchSpec
from .types import PhraseCombine, SearchOptions

# ------------------------ #


def effective_phrase_combine(options: SearchOptions | None) -> PhraseCombine:
    """Return ``phrase_combine``, defaulting to ``any`` (disjunction)."""

    raw = (options or {}).get("phrase_combine", "any")
    return raw if raw in ("any", "all") else "any"


# ....................... #


def normalize_search_queries(query: str | Sequence[str]) -> tuple[str, ...]:
    """Return non-empty stripped sub-queries for backend OR semantics.

    * A single :class:`str` that is empty or whitespace-only yields ``()`` (filter-only).
    * A :class:`~collections.abc.Sequence` of strings drops empty / whitespace-only
      entries; if none remain, ``()`` (filter-only).
    * ``str`` is **not** iterated as a sequence of characters (``str`` is handled first).

    :param query: One full-text string or several alternative phrases (OR).
    :returns: Tuple of sub-queries to combine with disjunction in the backend.
    """

    if isinstance(query, str):
        s = query.strip()
        return () if not s else (s,)

    parts: list[str] = []

    for item in query:
        s = str(item).strip()
        if s:
            parts.append(s)

    return tuple(parts)


# ....................... #


def calculate_effective_field_weights(
    spec: SearchSpec[Any],
    options: SearchOptions | None = None,
) -> dict[str, float]:
    """Resolve per-field FTS-style weights from spec defaults and caller options."""

    options = options or {}
    provided_weights = options.get("weights", {})
    fields_to_search = list(options.get("fields", []))

    if provided_weights:
        weights = {f: provided_weights.get(f, 0.0) for f in spec.fields}

    elif fields_to_search:
        weights = {f: 1.0 if f in fields_to_search else 0.0 for f in spec.fields}

    elif spec.default_weights:
        weights = dict(spec.default_weights)

    else:
        weights = {f: 1.0 for f in spec.fields}

    return weights
