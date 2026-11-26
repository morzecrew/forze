"""Wire-protocol strategies for served-model endpoints."""

from forze_inference.records import wrap_scalar_predictions

from .anthropic_messages import (
    MESSAGES_REQUEST_KEYS,
    AnthropicMessagesProtocol,
    messages_output_schema,
)
from .base import WireProtocol, WireRequest
from .generation import (
    CONTENT_REFUSED_CODE,
    REPLY_SHAPE_KEYS,
    InferenceOutputMode,
    PromptTemplate,
    copy_extra_body,
    validate_extra_body,
    validate_prompt_template,
    validate_text_output,
)
from .kserve_v2 import KserveV2Protocol, validate_flat_scalar_fields
from .mlflow import MlflowProtocol
from .openai_chat import (
    CHAT_REQUEST_KEYS,
    ChatTokenCapField,
    InferenceStructuredOutput,
    OpenAiChatProtocol,
    chat_output_schema,
    json_object_instruction,
)

# ----------------------- #

__all__ = [
    "CHAT_REQUEST_KEYS",
    "CONTENT_REFUSED_CODE",
    "MESSAGES_REQUEST_KEYS",
    "REPLY_SHAPE_KEYS",
    "AnthropicMessagesProtocol",
    "ChatTokenCapField",
    "InferenceOutputMode",
    "InferenceStructuredOutput",
    "KserveV2Protocol",
    "MlflowProtocol",
    "OpenAiChatProtocol",
    "PromptTemplate",
    "WireProtocol",
    "WireRequest",
    "chat_output_schema",
    "copy_extra_body",
    "json_object_instruction",
    "messages_output_schema",
    "validate_extra_body",
    "validate_flat_scalar_fields",
    "validate_prompt_template",
    "validate_text_output",
    "wrap_scalar_predictions",
]
