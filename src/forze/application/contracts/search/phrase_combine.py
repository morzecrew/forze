"""Resolve :attr:`~forze.application.contracts.search.SearchOptions.phrase_combine`."""

from .types import PhraseCombine, SearchOptions

# ------------------------ #


def effective_phrase_combine(options: SearchOptions | None) -> PhraseCombine:
    """Return ``phrase_combine``, defaulting to ``any`` (disjunction)."""

    raw = (options or {}).get("phrase_combine", "any")
    return raw if raw in ("any", "all") else "any"
