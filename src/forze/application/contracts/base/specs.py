from enum import StrEnum
from functools import cached_property
from typing import Any

import attrs

from forze.base.primitives import StrKeyNamespace
from forze.base.serialization import ModelCodec

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class BaseSpec:
    """Base resource specification."""

    name: str | StrEnum
    """Logical name for the resource."""

    # ....................... #

    @cached_property
    def default_namespace(self) -> StrKeyNamespace:
        """Default namespace for the resource."""

        return StrKeyNamespace(prefix=self.name)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MessageCodecSpec[M](BaseSpec):
    """Base specification binding a messaging namespace to its payload record codec.

    Shared by queue, pubsub, and stream specs; each only narrows the docstring.
    """

    codec: ModelCodec[M, Any]
    """Payload record codec for messages in this namespace."""

    # ....................... #

    @property
    def model_type(self) -> type[M]:
        """Payload model type carried by :attr:`codec`."""

        return self.codec.model_type
