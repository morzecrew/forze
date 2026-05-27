"""Property tests for keyset v1 encode/decode roundtrip."""

from __future__ import annotations

import hypothesis.strategies as st
from hypothesis import given, settings

from forze.application.contracts.querying import decode_keyset_v1, encode_keyset_v1


@settings(max_examples=25, deadline=None)
@given(
    sort_keys=st.lists(st.text(min_size=1, max_size=8), min_size=1, max_size=4),
    directions=st.lists(st.sampled_from(["asc", "desc"]), min_size=1, max_size=4),
    values=st.lists(st.text(min_size=0, max_size=16), min_size=1, max_size=4),
)
def test_keyset_v1_roundtrip(
    sort_keys: list[str],
    directions: list[str],
    values: list[str],
) -> None:
    n = min(len(sort_keys), len(directions), len(values))
    keys = sort_keys[:n]
    dirs = directions[:n]
    vals = values[:n]

    token = encode_keyset_v1(sort_keys=keys, directions=dirs, values=vals)
    decoded_keys, decoded_dirs, decoded_vals = decode_keyset_v1(token)

    assert decoded_keys == keys
    assert decoded_dirs == dirs
    assert decoded_vals == vals
