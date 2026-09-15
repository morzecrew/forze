"""Route configs for served-model inference over HTTP."""

from typing import Any, Literal, final, get_args

import attrs

from forze.application.contracts.egress import require_egress_acknowledged
from forze.application.contracts.inference import InferenceSpec
from forze.application.contracts.resolution import NamedResourceSpec
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc

from ...protocols import (
    InferenceOutputMode,
    KserveV2Protocol,
    MlflowProtocol,
    OpenAiChatProtocol,
    PromptTemplate,
    WireProtocol,
    chat_output_schema,
    validate_flat_scalar_fields,
    validate_prompt_template,
    validate_text_output,
)

# ----------------------- #

InferenceWireProtocolName = Literal["kserve_v2", "mlflow", "openai_chat"]
"""Supported serving dialects (JSON-record scope)."""

_PROTOCOL_NAMES = frozenset(get_args(InferenceWireProtocolName))
_OUTPUT_MODES = frozenset(get_args(InferenceOutputMode))
"""The closed sets behind the two literal annotations, read off the aliases so they cannot
drift. `attrs` does not enforce a `Literal` at runtime, and neither value fails loudly on
its own: an unknown protocol reaches `wire_protocol()` as an internal error, and an unknown
output mode makes the encoder send no constraint while the decoder still expects JSON."""

_GENERATION_FIELDS = ("prompt", "output_mode", "temperature", "max_output_tokens")
"""Fields only the ``openai_chat`` dialect reads. Refused on the others rather than
ignored: a prompt on a KServe route is a wiring mistake, and silently dropping it would
send the model a request the operator believes was shaped by it."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class HttpInferenceConfig(TenantAwareIntegrationConfig):
    """Wiring config for one served-model inference route.

    Features cross the process (and usually the network) boundary **in plaintext** — the
    model needs real values, so field encryption cannot apply. The operator must state
    that consciously via :attr:`acknowledge_data_egress`; wiring fails closed until then.
    """

    protocol: InferenceWireProtocolName
    """Which wire dialect the endpoint speaks. ``kserve_v2`` covers KServe, mlserver,
    Seldon and Triton's HTTP frontend; ``mlflow`` is the legacy ``/invocations`` scoring
    protocol; ``openai_chat`` covers anything speaking ``/v1/chat/completions``."""

    model_name: NamedResourceSpec
    """Server-side model id — a static name or a ``(tenant_id) -> name`` resolver for
    per-tenant models (namespace-tier isolation)."""

    acknowledge_data_egress: bool = False
    """Must be ``True``: an explicit statement that this route sends feature values in
    plaintext to an external endpoint."""

    max_batch_size: int | None = None
    """Hard per-call instance cap the endpoint imposes, or ``None`` for unbounded.
    ``predict_many`` refuses an oversized batch whole; ``predict_stream`` sub-batches
    its wire calls to the cap."""

    deterministic: bool = False
    """Declare that the served model returns the same output for the same input
    (advertised via capabilities; the adapter cannot verify it)."""

    prompt: PromptTemplate | None = None
    """The generation prompt, required by ``openai_chat`` and refused by the others.

    Prompt-shaped configuration lives here rather than in handler code for the reason the
    procedure plane keeps its SQL in wiring: what the model is asked is a reviewed
    deployment fact, and a handler that passes a typed instance cannot drift from it."""

    output_mode: InferenceOutputMode | None = None
    """``openai_chat`` only; defaults to ``structured``.

    ``structured`` constrains the completion to the output model's JSON schema, so the
    route answers with a validated ``Out``. ``text`` takes completion prose into a
    one-field ``str`` output model."""

    temperature: float | None = None
    """``openai_chat`` only: sampling temperature, omitted when unset.

    Sampling is configuration, never a per-call option — a route's behaviour is a reviewed
    wiring fact, the same stance the port takes on which model answers."""

    max_output_tokens: int | None = None
    """``openai_chat`` only: completion-length ceiling, omitted when unset."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        self._validate_vocabulary()
        self._validate_limits()

        # Sensitivity is not a choice here: a served model needs real values, so a route
        # always carries them out. The shared gate is called with that fixed, which is why
        # this config exposes no `egress_sensitive` knob to turn it off.
        require_egress_acknowledged(
            subject="HttpInferenceConfig",
            detail=(
                "this route sends feature values in plaintext to an external endpoint, "
                "and the operator must state that consciously."
            ),
            egress_sensitive=True,
            acknowledged=self.acknowledge_data_egress,
        )

        self._validate_dialect_pairing()

    # ....................... #

    def _validate_vocabulary(self) -> None:
        """The two closed sets `attrs` does not enforce at runtime."""

        if self.protocol not in _PROTOCOL_NAMES:
            raise exc.configuration(
                f"HttpInferenceConfig.protocol={self.protocol!r} is not a wire dialect this "
                f"plane speaks; choose one of {', '.join(sorted(_PROTOCOL_NAMES))}."
            )

        if self.output_mode is not None and self.output_mode not in _OUTPUT_MODES:
            raise exc.configuration(
                f"HttpInferenceConfig.output_mode={self.output_mode!r} is not a generation "
                f"mode; choose one of {', '.join(sorted(_OUTPUT_MODES))}."
            )

    # ....................... #

    def _validate_limits(self) -> None:
        """Numeric bounds, each refused at wiring rather than by the endpoint."""

        # Both are sent to the provider verbatim, and both have a value the endpoint
        # rejects: a rejection then reaches the caller as a wire mismatch, after the
        # request was made. Refused at wiring, the way the batch cap is.
        if self.max_output_tokens is not None and self.max_output_tokens < 1:
            raise exc.configuration(
                f"HttpInferenceConfig.max_output_tokens={self.max_output_tokens} must be at "
                "least 1; omit it to leave the ceiling to the endpoint."
            )

        if self.temperature is not None and self.temperature < 0:
            raise exc.configuration(
                f"HttpInferenceConfig.temperature={self.temperature} must not be negative. "
                "The upper bound is the provider's (2 for OpenAI, 1 for Anthropic) and is "
                "not checked here."
            )

        # Caught here rather than at the first stream call: a cap below 1 makes
        # predict_many refuse everything and predict_stream have no servable sub-batch,
        # so it is a wiring mistake and should cost a boot, not a request.
        if self.max_batch_size is not None and self.max_batch_size < 1:
            raise exc.configuration(
                f"HttpInferenceConfig.max_batch_size={self.max_batch_size} must be at "
                "least 1; omit it (None) for an endpoint with no batch limit."
            )

    # ....................... #

    def _validate_dialect_pairing(self) -> None:
        # Fail-closed in both directions. A chat route with no prompt has nothing to ask
        # the model; a prompt on a scoring route is a field nothing reads, and an ignored
        # field is indistinguishable from an applied one from the outside.
        if self.protocol == "openai_chat":
            if self.prompt is None:
                raise exc.configuration(
                    "HttpInferenceConfig(protocol='openai_chat') requires prompt=..., "
                    "the template the route sends for every instance."
                )

        else:
            offending = [field for field in _GENERATION_FIELDS if getattr(self, field) is not None]

            if offending:
                raise exc.configuration(
                    f"HttpInferenceConfig(protocol={self.protocol!r}) does not read "
                    f"{', '.join(offending)}; {'that field is' if len(offending) == 1 else 'those fields are'} "
                    "read by the openai_chat dialect only."
                )

    # ....................... #

    def validate_against_spec(self, spec: InferenceSpec[Any, Any]) -> None:
        """Fail-closed spec↔config check, run by the factory at resolve time."""

        if self.protocol == "kserve_v2":
            validate_flat_scalar_fields(spec)

        if self.protocol == "openai_chat" and self.prompt is not None:
            validate_prompt_template(spec, self.prompt)

            if self._output_mode == "text":
                validate_text_output(spec)

            else:
                # Derived here for its refusals only — the encoder derives it again per
                # request from the same spec. A schema the constraint cannot express costs
                # a resolve, not a production request that answers with unconstrained prose.
                chat_output_schema(spec)

    # ....................... #

    @property
    def _output_mode(self) -> InferenceOutputMode:
        return self.output_mode if self.output_mode is not None else "structured"

    # ....................... #

    def wire_protocol(self) -> WireProtocol:
        if self.protocol == "kserve_v2":
            return KserveV2Protocol()

        if self.protocol == "mlflow":
            return MlflowProtocol()

        if self.prompt is None:  # pragma: no cover - refused in __attrs_post_init__
            raise exc.internal(
                "HttpInferenceConfig(protocol='openai_chat') reached wire_protocol() "
                "without a prompt."
            )

        return OpenAiChatProtocol(
            prompt=self.prompt,
            output_mode=self._output_mode,
            temperature=self.temperature,
            max_output_tokens=self.max_output_tokens,
        )
