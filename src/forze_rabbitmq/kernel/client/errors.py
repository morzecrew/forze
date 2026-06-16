from forze_rabbitmq._compat import require_rabbitmq

require_rabbitmq()

# ....................... #

from typing import Any, Mapping

from aio_pika import exceptions as aio_pika_errors

from forze.base.conformity import static_fn_conformity
from forze.base.exceptions import (
    CoreException,
    ExceptionInterceptor,
    ExceptionMapper,
    default_chain_exc_mapper,
    fallback_exception_mapper,
)

# ----------------------- #

_fallback = fallback_exception_mapper("RabbitMQ")

# ....................... #


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract]
def _rabbitmq_eh(  # skipcq: PY-R1000
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    match exc:
        case CoreException():
            return exc

        case aio_pika_errors.AuthenticationError():
            return CoreException.infrastructure(
                "RabbitMQ authentication failed.",
                details=details,
            )

        case aio_pika_errors.IncompatibleProtocolError():
            return CoreException.infrastructure(
                "RabbitMQ protocol mismatch.",
                details=details,
            )

        case aio_pika_errors.AMQPConnectionError():
            return CoreException.infrastructure(
                "RabbitMQ connection error.",
                details=details,
            )

        case aio_pika_errors.ChannelInvalidStateError():
            return CoreException.infrastructure(
                "RabbitMQ channel is in an invalid state.",
                details=details,
            )

        case aio_pika_errors.AMQPChannelError():
            return CoreException.infrastructure(
                "RabbitMQ channel error.",
                details=details,
            )

        case aio_pika_errors.PublishError() | aio_pika_errors.DeliveryError():
            return CoreException.infrastructure(
                "RabbitMQ message delivery failed.",
                details=details,
            )

        case aio_pika_errors.MessageProcessError():
            return CoreException.infrastructure(
                "RabbitMQ message processing failed.",
                details=details,
            )

        case TimeoutError():
            return CoreException.infrastructure(
                "RabbitMQ operation timed out.",
                details=details,
            )

        case _:
            return _fallback(exc, site=site, details=details)


# ....................... #

_rabbitmq_chain = default_chain_exc_mapper.chain(_rabbitmq_eh)
exc_interceptor = ExceptionInterceptor(mapper=_rabbitmq_chain)
