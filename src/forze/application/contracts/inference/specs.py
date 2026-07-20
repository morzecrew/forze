"""Declarative specification for one typed inference task."""

from typing import Any, final

import attrs
from pydantic import BaseModel

from forze.base.exceptions import exc
from forze.base.serialization import (
    ModelCodec,
    default_model_codec,
    stored_field_names_for,
)

from ..base import BaseSpec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class InferenceSpec[In: BaseModel, Out: BaseModel](BaseSpec):
    """Specification for one logical inference task.

    **One spec = one model** (like :class:`~forze.application.contracts.procedure.ProcedureSpec`,
    unlike :class:`~forze.application.contracts.analytics.AnalyticsSpec`'s named-query map): each
    model is a heterogeneous unit with its own input and output shape, so a keyed registry inside
    one spec could not give each entry its own types. The spec names the *task* (``fraud_scorer``),
    never the physical model — the artifact loader, endpoint, or model URI lives in the wiring
    config of whichever adapter serves the route, so swapping a local artifact for a served or
    cloud model is a wiring change with zero handler edits.

    Both types are Pydantic models. A scalar prediction wraps in a one-field model — this keeps
    the codec path uniform across adapters. Tensor-shaped payloads are plain ``Sequence[float]``
    (or nested lists) inside the models; binary tensor encodings are a wire concern of individual
    adapters and never appear in the contract.
    """

    input: type[In]
    """Pydantic model for one inference instance passed to ``predict*``."""

    output: type[Out]
    """Pydantic model for one prediction returned by ``predict*``."""

    input_codec: ModelCodec[In, Any] | None = attrs.field(
        default=None,
        eq=False,
        repr=False,
    )
    """Input codec; defaults to :func:`default_model_codec` for :attr:`input`."""

    output_codec: ModelCodec[Out, Any] | None = attrs.field(
        default=None,
        eq=False,
        repr=False,
    )
    """Output codec; defaults to :func:`default_model_codec` for :attr:`output`."""

    capture_inputs: bool = attrs.field(default=False)
    """Allow simulation value capture to record input values verbatim.

    Off by default, and deliberately so: features cross to the model in plaintext (they cannot be
    field-encrypted — that is why external routes must acknowledge data egress) and are typically
    PII-dense, so every input field is masked on captured traces unless an author opts in. Capture
    only happens under runtime tracing / simulation — production traces stay id-only either way —
    but a DST bundle is still an artifact that gets stored and shared."""

    description: str | None = attrs.field(default=None)
    """Optional human-readable description for documentation."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        validate_inference_spec(self)

    # ....................... #

    @property
    def sensitive_capture_fields(self) -> frozenset[str]:
        """Input fields masked in simulation value capture (see :attr:`capture_inputs`).

        Read duck-typed by the port-instrumentation layer, which unions it with the encryption
        signal — inference declares every input field rather than a subset, because it has no
        encryption declaration to derive one from.
        """

        if self.capture_inputs:
            return frozenset()

        return stored_field_names_for(self.input)

    # ....................... #

    @property
    def resolved_input_codec(self) -> ModelCodec[In, Any]:
        """Input codec (explicit override or :func:`default_model_codec`)."""

        if self.input_codec is not None:
            return self.input_codec

        return default_model_codec(self.input)

    # ....................... #

    @property
    def resolved_output_codec(self) -> ModelCodec[Out, Any]:
        """Output codec (explicit override or :func:`default_model_codec`)."""

        if self.output_codec is not None:
            return self.output_codec

        return default_model_codec(self.output)


# ....................... #


def validate_inference_spec(spec: InferenceSpec[Any, Any]) -> None:
    """Check internal consistency; raise on violation.

    :param spec: Inference specification to validate.
    """

    if not (
        isinstance(spec.input, type)  # pyright: ignore[reportUnnecessaryIsInstance]
        and issubclass(spec.input, BaseModel)
    ):
        raise exc.configuration("InferenceSpec.input must be a Pydantic BaseModel subclass.")

    if not (
        isinstance(spec.output, type)  # pyright: ignore[reportUnnecessaryIsInstance]
        and issubclass(spec.output, BaseModel)
    ):
        raise exc.configuration(
            "InferenceSpec.output must be a Pydantic BaseModel subclass — wrap a scalar "
            "prediction in a one-field model."
        )
