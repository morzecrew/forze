"""Wire-protocol strategies for served-model endpoints."""

from forze_inference.records import wrap_scalar_predictions

from .base import WireProtocol, WireRequest
from .kserve_v2 import KserveV2Protocol, validate_flat_scalar_fields
from .mlflow import MlflowProtocol
from .openai_chat import (
    CONTENT_REFUSED_CODE,
    InferenceOutputMode,
    OpenAiChatProtocol,
    PromptTemplate,
    chat_output_schema,
    validate_prompt_template,
    validate_text_output,
)

# ----------------------- #

__all__ = [
    "CONTENT_REFUSED_CODE",
    "InferenceOutputMode",
    "KserveV2Protocol",
    "MlflowProtocol",
    "OpenAiChatProtocol",
    "PromptTemplate",
    "WireProtocol",
    "WireRequest",
    "chat_output_schema",
    "validate_flat_scalar_fields",
    "validate_prompt_template",
    "validate_text_output",
    "wrap_scalar_predictions",
]
