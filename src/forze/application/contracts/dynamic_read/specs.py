"""Declarative specification for one governed dynamic-read surface."""

from typing import final

import attrs

from forze.base.exceptions import exc

from ..base import BaseSpec

# ----------------------- #

STATEMENT_CAPTURE_KEY = "statement"
"""Trace key the runtime-authored statement text is captured under.

Shared by :attr:`DynamicReadSpec.trace_text_arg_key` (what the tracing proxy records the
leading text argument as) and :attr:`DynamicReadSpec.sensitive_capture_fields` (what it
masks), so the two can never name different things and leave the text unmasked.
"""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DynamicReadSpec(BaseSpec):
    """Specification for one governed dynamic-read surface (a route, not a statement).

    The plane for statements **whose text is data**: SQL authored at runtime by a catalog, a
    semantic-layer compiler, or a generator, rather than registered at wiring. There are no
    ``In``/``Out`` generics because both the parameter shape and the row shape are runtime
    data — that is the plane's definition, and the reason the framework, not the caller, owns
    read-only enforcement, tenancy confinement and resource limits for it.

    One spec = one *surface*: every statement executed through it shares these caps, the
    route's resilience policy, and the route's wiring-declared provenance and confinement (see
    the backend config, e.g.
    :class:`~forze_postgres.execution.deps.configs.PostgresDynamicReadConfig`).

    **No encryption field, deliberately.** A dynamic statement's output shape is unknowable, so
    a field-encrypted column comes back as ciphertext and a statement may even ``ORDER BY``
    one — ciphertext order is a silent wrong answer. Point this plane at analytics-shaped
    relations that carry no sealed columns; a wiring-time check is impossible (there is no
    statement to inspect) and pretending otherwise would be worse than the honest boundary.
    """

    row_cap: int = attrs.field(default=10_000)
    """Hard ceiling on rows a single execution may return.

    Exceeding it **raises** (``dynamic_read_row_cap_exceeded``) rather than truncating: a
    silently truncated result reads as "the data is small" and renders a confidently-wrong
    dashboard. There is no ``unlimited`` spelling — a caller who wants more sets a bigger
    number and owns it in review."""

    max_statement_bytes: int = attrs.field(default=65_536)
    """Ceiling on the UTF-8 length of a statement, checked before the connection is touched."""

    capture_statements: bool = attrs.field(default=False)
    """Allow simulation value capture to record statement text verbatim.

    Off by default (the inference ``capture_inputs`` twin): a runtime statement embeds the
    literals it was compiled with — filter values, identifiers, sometimes user input — so the
    text is masked on captured traces unless an author opts in. Capture only happens under
    runtime tracing / simulation; production traces stay id-only either way, but a DST bundle
    is still an artifact that gets stored and shared."""

    description: str | None = attrs.field(default=None)
    """Optional human-readable description for documentation."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        validate_dynamic_read_spec(self)

    # ....................... #

    @property
    def trace_text_arg_key(self) -> str:
        """Key a leading *text* argument is captured under on traced calls.

        Read duck-typed by the port-instrumentation layer. Without it a statement string is
        unstructured to the tracer and captured as nothing at all — which is safe but leaves
        :attr:`capture_statements` with nothing to turn on.
        """

        return STATEMENT_CAPTURE_KEY

    # ....................... #

    @property
    def sensitive_capture_fields(self) -> frozenset[str]:
        """Captured fields masked in simulation value capture (see :attr:`capture_statements`).

        Read duck-typed by the port-instrumentation layer, which unions it with the encryption
        signal. Masked rather than dropped so a trace consumer can tell "a statement ran, its
        text was withheld" from "no statement was recorded".
        """

        if self.capture_statements:
            return frozenset()

        return frozenset({STATEMENT_CAPTURE_KEY})


# ....................... #


def validate_dynamic_read_spec(spec: DynamicReadSpec) -> None:
    """Check internal consistency; raise on violation.

    :param spec: Dynamic-read surface to validate.
    """

    if spec.row_cap < 1:
        raise exc.configuration(
            "DynamicReadSpec.row_cap must be at least 1.",
            code="dynamic_read_row_cap_invalid",
            details={"route": str(spec.name), "row_cap": spec.row_cap},
        )

    if spec.max_statement_bytes < 1:
        raise exc.configuration(
            "DynamicReadSpec.max_statement_bytes must be at least 1.",
            code="dynamic_read_statement_bytes_invalid",
            details={"route": str(spec.name), "max_statement_bytes": spec.max_statement_bytes},
        )
