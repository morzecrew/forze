"""Tests for :func:`~forze.application.contracts.search.normalize_search_queries`."""

from forze.application.contracts.search import normalize_search_queries


def test_normalize_single_string() -> None:
    assert normalize_search_queries("  hello  ") == ("hello",)
    assert normalize_search_queries("") == ()
    assert normalize_search_queries("   ") == ()


def test_normalize_sequence_drops_blanks() -> None:
    assert normalize_search_queries(["  a  ", "", "b"]) == ("a", "b")
    assert normalize_search_queries([]) == ()
    assert normalize_search_queries(["", "  "]) == ()


def test_normalize_sequence_single_element_equals_string() -> None:
    assert normalize_search_queries(["foo"]) == normalize_search_queries("foo")


def test_normalize_str_not_iterated_as_chars() -> None:
    assert normalize_search_queries("ab") == ("ab",)
    assert len(normalize_search_queries("ab")) == 1
