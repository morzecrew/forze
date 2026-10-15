"""The wrappers a deps registry can put around every port it resolves."""

from opentelemetry.sdk.trace import TracerProvider

from forze.application.execution import DepsRegistry, ExecutionContext

# ----------------------- #


def with_port_wrappers(registry: DepsRegistry) -> DepsRegistry:
    """*registry* with OTel port spans and port logging, as production can wire it: a
    configurable port it resolves comes back inside two proxies."""

    return registry.with_otel_port_spans(
        tracer=TracerProvider().get_tracer("test")
    ).with_port_logging()


# ....................... #


def traced_ops(ctx: ExecutionContext) -> set[str]:
    """The port methods called through the runtime-tracing wrapper in this task, for a
    registry built ``.with_tracing(runtime=True)``: a call made on the backend behind the
    wrappers is not among them."""

    trace = ctx.deps.runtime_trace()
    assert trace is not None, "the registry was not built with runtime tracing"

    return {event.op for event in trace.events}
