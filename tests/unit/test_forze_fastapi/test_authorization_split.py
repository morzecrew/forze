"""Unit tests for the authorization-header splitter."""

import pytest

from forze_fastapi.security.resolvers import _split_authorization

# ----------------------- #


@pytest.mark.parametrize(
    ("raw", "sep", "expected"),
    [
        # Empty / whitespace-only must not raise IndexError.
        ("", " ", ("", None)),
        ("   ", " ", ("", None)),
        ("", ":", ("", None)),
        # Whitespace scheme split (Bearer), collapsing runs.
        ("Bearer abc", " ", ("Bearer", "abc")),
        ("Bearer  abc", " ", ("Bearer", "abc")),
        ("Bearer", " ", ("Bearer", None)),
        # Colon split for prefix:key API keys (first colon only).
        ("sk_live:secret", ":", ("sk_live", "secret")),
        ("sk:live:secret", ":", ("sk", "live:secret")),
        ("barekey", ":", ("barekey", None)),
        (":", ":", ("", "")),
    ],
)
def test_split_authorization(
    raw: str, sep: str, expected: tuple[str, str | None]
) -> None:
    assert _split_authorization(raw, sep=sep) == expected
