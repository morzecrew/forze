"""Query-string helpers for Mongo search pipelines."""

from forze.application.contracts.search import PhraseCombine

# ----------------------- #


def build_text_search_string(
    terms: tuple[str, ...],
    *,
    combine: PhraseCombine,
) -> str:
    """Build a ``$text`` search string from normalized query terms.

    ``all`` requires every term (quoted tokens). ``any`` uses space-separated OR semantics.
    """

    if not terms:
        return ""

    if combine == "all":
        return " ".join(f'"{t}"' for t in terms)

    return " ".join(terms)
