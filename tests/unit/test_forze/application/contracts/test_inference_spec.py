"""Unit tests for the inference contract spec, capabilities, and dep key."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.inference import (
    DEFAULT_INFERENCE_CAPABILITIES,
    FULL_INFERENCE_CAPABILITIES,
    UNSUPPORTED_INFERENCE_FEATURE_CODE,
    InferenceCapabilities,
    InferenceDepKey,
    InferenceSpec,
    validate_batch_size,
    validate_stream_supported,
)
from forze.base.exceptions import CoreException

# ----------------------- #


class _Features(BaseModel):
    x: float = 0.0


class _Score(BaseModel):
    y: float = 0.0


# ....................... #


class TestInferenceSpec:
    def test_valid_spec(self) -> None:
        spec = InferenceSpec(name="scorer", input=_Features, output=_Score)
        assert spec.input is _Features
        assert spec.output is _Score

    def test_resolved_codecs_default(self) -> None:
        spec = InferenceSpec(name="scorer", input=_Features, output=_Score)
        assert spec.resolved_input_codec.model_type is _Features
        assert spec.resolved_output_codec.model_type is _Score

    def test_non_model_input_raises(self) -> None:
        with pytest.raises(CoreException, match="BaseModel"):
            InferenceSpec(name="bad", input=int, output=_Score)  # type: ignore[arg-type]

    def test_non_model_output_raises(self) -> None:
        with pytest.raises(CoreException, match="one-field model"):
            InferenceSpec(name="bad", input=_Features, output=float)  # type: ignore[arg-type]


# ....................... #


class TestInferenceCapabilities:
    def test_defaults_are_narrowest(self) -> None:
        caps = DEFAULT_INFERENCE_CAPABILITIES
        assert not caps.native_batch
        assert caps.max_batch_size is None
        assert not caps.supports_stream
        assert not caps.supports_async_jobs
        assert not caps.deterministic

    def test_full_is_superset(self) -> None:
        caps = FULL_INFERENCE_CAPABILITIES
        assert caps.native_batch
        assert caps.supports_stream
        assert caps.supports_async_jobs
        assert caps.deterministic

    def test_stream_refused_fail_closed(self) -> None:
        with pytest.raises(CoreException) as ei:
            validate_stream_supported(DEFAULT_INFERENCE_CAPABILITIES, backend="test")
        assert ei.value.code == UNSUPPORTED_INFERENCE_FEATURE_CODE

    def test_stream_allowed_when_declared(self) -> None:
        validate_stream_supported(FULL_INFERENCE_CAPABILITIES, backend="test")

    def test_batch_cap_refuses_oversized_whole(self) -> None:
        caps = InferenceCapabilities(max_batch_size=2)
        validate_batch_size(caps, 2, backend="test")
        with pytest.raises(CoreException) as ei:
            validate_batch_size(caps, 3, backend="test")
        assert ei.value.code == UNSUPPORTED_INFERENCE_FEATURE_CODE

    def test_uncapped_batch_passes(self) -> None:
        validate_batch_size(DEFAULT_INFERENCE_CAPABILITIES, 10_000, backend="test")


# ....................... #


class TestInferenceDepKey:
    def test_key_name_drives_port_metadata(self) -> None:
        # domain="inference", phase="query" are inferred from the key name by the
        # port-instrumentation layer; the name is load-bearing, not cosmetic.
        assert InferenceDepKey.name == "inference_query"
