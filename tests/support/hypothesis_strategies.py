"""Hypothesis strategies for pure-function property tests (not live DB)."""

from __future__ import annotations

import hypothesis.strategies as st
from hypothesis import settings

# Stable CI profile for property tests in unit modules.
integration_hypothesis_settings = settings(max_examples=25, deadline=None)

pagination_limit = st.integers(min_value=1, max_value=500)
pagination_offset = st.integers(min_value=0, max_value=10_000)

simple_filter_values = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-1_000, max_value=1_000),
    st.text(min_size=0, max_size=32),
)

phrase_list = st.lists(
    st.text(min_size=1, max_size=24),
    min_size=0,
    max_size=5,
)

phrase_combine = st.sampled_from(["all", "any"])
