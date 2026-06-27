"""Well-known transport header names for cross-boundary envelope propagation.

Queue, stream, and pubsub messages carry an optional string-to-string
``headers`` mapping that rides the transport's native metadata channel
(AMQP headers, SQS message attributes, the Redis JSON envelope). The
constants below are the **well-known header names** used by the outbox
relay to propagate the staged integration-event envelope across the broker
boundary, and by consumers (``process_with_inbox``) to rebind invocation
metadata so correlation survives the hop.

These are *transport header names*, deliberately decoupled from the
structlog field names in :mod:`forze.application.execution.context.invocation`
(``CORR_ID_KEY`` and friends): log field names and wire header names evolve
independently. They follow the existing ``forze_*`` convention used by
transport-internal attributes (``forze_type``, ``forze_key``, ...).

Collision rule (uniform across transports): caller-supplied header keys
pass through verbatim, but the transport-internal reserved keys
(``forze_type``, ``forze_key``, ``forze_encoding``, ``forze_enqueued_at``)
always win — a caller header under one of those names is overwritten by the
transport's own value and never round-trips. The envelope keys below are
ordinary caller headers and round-trip unchanged.

Trust model: headers are plain broker metadata. Within a deployment they
are written by the application's own relay, but any producer with broker
access can forge them — consumers must treat header-derived identity
(notably the tenant header) as **untrusted input** and only honor it behind
an explicit opt-in.
"""

from typing import Final

# ----------------------- #

HEADER_CORRELATION_ID: Final = "forze_correlation_id"
"""Correlation id of the originating invocation (UUID string)."""

HEADER_CAUSATION_ID: Final = "forze_causation_id"
"""Causation id of the originating invocation (UUID string)."""

HEADER_EXECUTION_ID: Final = "forze_execution_id"
"""Execution id of the originating invocation (UUID string)."""

HEADER_TENANT_ID: Final = "forze_tenant_id"
"""Tenant scope of the staged event (UUID string)."""

HEADER_EVENT_ID: Final = "forze_event_id"
"""Integration event id — the consumer-side dedup key (UUID string)."""

HEADER_OCCURRED_AT: Final = "forze_occurred_at"
"""When the event occurred (ISO-8601 string)."""

HEADER_HLC: Final = "forze_hlc"
"""Hybrid Logical Clock stamp (``physical_ms.logical`` string, lexsortable) —
present only from HLC-ordering outbox backends. A consumer merges it into its
process clock so reactions causally follow their cause, and may order on it."""

# ....................... #

ENVELOPE_HEADER_KEYS: Final = frozenset(
    {
        HEADER_CORRELATION_ID,
        HEADER_CAUSATION_ID,
        HEADER_EXECUTION_ID,
        HEADER_TENANT_ID,
        HEADER_EVENT_ID,
        HEADER_OCCURRED_AT,
        HEADER_HLC,
    }
)
"""All well-known *forze* envelope header names (the dedup/correlation contract)."""

# ....................... #

HEADER_TRACEPARENT: Final = "traceparent"
"""W3C trace-context ``traceparent`` — present only from trace-propagation outbox backends. Carries
the publishing operation's span identity so the consumer (``process_with_inbox``) links its span to
the publish span, stitching the async outbox→broker→inbox flow into one distributed trace.

The **standard W3C header name** (not ``forze_``-prefixed), so OTel collectors and tracing tools that
sniff broker headers recognize it — and deliberately **not** part of :data:`ENVELOPE_HEADER_KEYS`,
which is the forze-internal correlation contract. Trust model: like every header, plain broker
metadata a producer with broker access can forge; it only ever influences trace parenting, never
identity or authorization."""

# ....................... #

HTTP_HEADER_DEADLINE_BUDGET: Final = "X-Forze-Deadline-Budget"
"""Remaining invocation time budget in seconds (decimal string) — HTTP-only.

Forwarded on synchronous service-to-service hops (the outbound HTTP adapter
attaches the caller's :func:`remaining_time` automatically) and honored by the
receiving boundary only behind an explicit opt-in; binding is tighten-only, so
a forged value can shorten the receiver's work but never extend a deadline.
The budget is a *duration*, never an absolute instant — replica clocks diverge.

Deliberately **not** part of :data:`ENVELOPE_HEADER_KEYS`: deadlines belong to
the synchronous call chain. A queued event consumed after a backlog must not
inherit its producer's leftover budget — it would expire legitimate work.
"""

# ....................... #

__all__ = [
    "HEADER_CORRELATION_ID",
    "HEADER_CAUSATION_ID",
    "HEADER_EXECUTION_ID",
    "HEADER_TENANT_ID",
    "HEADER_EVENT_ID",
    "HEADER_OCCURRED_AT",
    "HEADER_HLC",
    "ENVELOPE_HEADER_KEYS",
    "HEADER_TRACEPARENT",
    "HTTP_HEADER_DEADLINE_BUDGET",
]
