"""Unit coverage for the PGroonga highlight builder + Python-side marking.

PGroonga highlighting selects the raw field text and wraps matches in Python (the shared
mock-oracle marker), because ``pgroonga_snippet_html``'s normalizer case-folds ASCII only —
so a lowercase query silently dropped mixed-case non-ASCII (e.g. Cyrillic) highlights.
"""

from typing import Any

from pydantic import BaseModel

from forze_postgres.adapters.search._highlights import (
    build_fts_highlight,
    build_pgroonga_highlight,
    extract_and_strip_highlights,
)

# ----------------------- #


class _Row(BaseModel):
    id: int
    name: str


def _spec() -> Any:
    from forze.application.contracts.search import SearchSpec

    return SearchSpec(name="orgs", model_type=_Row, fields=["name"])


def _rendered(select: Any) -> str:
    return " ".join(col.as_string(None) for col in select.columns)


# ....................... #


def test_pgroonga_highlight_selects_raw_field_not_snippet() -> None:
    select = build_pgroonga_highlight(
        spec=_spec(), options={"highlight": True}, terms=("бета",), alias="t"
    )

    assert select is not None
    assert select.engine == "pgroonga"
    # Raw field text, marked in Python: no snippet SQL, no bound params.
    assert "pgroonga_snippet_html" not in _rendered(select)
    assert "coalesce" in _rendered(select).lower()
    assert select.params == ()
    assert select.tokens == ("бета",)


def test_pgroonga_highlight_marks_cyrillic_preserving_case() -> None:
    select = build_pgroonga_highlight(
        spec=_spec(), options={"highlight": True}, terms=("бета",), alias="t"
    )
    assert select is not None

    # The data SELECT yields the raw field text under __hl__0; extraction wraps the match.
    rows = [{"id": 1, "name": "ООО", "__hl__0": 'ООО "БетаМед"'}]
    highlights = extract_and_strip_highlights(rows, select)

    assert highlights == [{"name": ('ООО "<em>Бета</em>Мед"',)}]
    # Synthetic column is stripped so the row decodes cleanly.
    assert "__hl__0" not in rows[0]


def test_pgroonga_highlight_no_match_maps_to_empty() -> None:
    select = build_pgroonga_highlight(
        spec=_spec(), options={"highlight": True}, terms=("гамма",), alias="t"
    )
    assert select is not None

    rows = [{"id": 1, "name": "ООО", "__hl__0": 'ООО "БетаМед"'}]
    assert extract_and_strip_highlights(rows, select) == [{}]


def test_fts_highlight_still_uses_ts_headline() -> None:
    select = build_fts_highlight(
        spec=_spec(), options={"highlight": True}, terms=("beta",), alias="t"
    )

    assert select is not None
    assert select.engine == "fts"
    assert "ts_headline" in _rendered(select)


def test_pgroonga_highlight_none_when_not_requested() -> None:
    assert (
        build_pgroonga_highlight(
            spec=_spec(), options={}, terms=("бета",), alias="t"
        )
        is None
    )
