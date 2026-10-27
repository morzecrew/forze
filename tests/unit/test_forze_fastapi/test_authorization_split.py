"""Unit tests for the authorization-header splitter."""

import pytest

from forze_fastapi.security.resolvers import _split_authorization

# ----------------------- #


# The ``prefix:key`` API-key split moved to ``ingress.api_key_credentials``, tested there.
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # Empty / whitespace-only must not raise IndexError.
        ("", ("", None)),
        ("   ", ("", None)),
        # Whitespace scheme split (Bearer), collapsing runs.
        ("Bearer abc", ("Bearer", "abc")),
        ("Bearer  abc", ("Bearer", "abc")),
        (" Bearer abc ", ("Bearer", "abc")),
        ("Bearer", ("Bearer", None)),
    ],
)
def test_split_authorization(raw: str, expected: tuple[str, str | None]) -> None:
    assert _split_authorization(raw) == expected
