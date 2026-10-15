"""The wrappers a deps registry can put around every port it resolves."""

from opentelemetry.sdk.trace import TracerProvider

from forze.application.execution import DepsRegistry

# ----------------------- #


def with_port_wrappers(registry: DepsRegistry) -> DepsRegistry:
    """*registry* with OTel port spans and port logging, as production can wire it: a
    configurable port it resolves comes back inside two proxies."""

    return registry.with_otel_port_spans(
        tracer=TracerProvider().get_tracer("test")
    ).with_port_logging()
