from forze_rabbitmq._compat import require_rabbitmq

require_rabbitmq()

# ....................... #

from functools import partial
from typing import Any

from aio_pika import exceptions as aio_pika_errors

from forze.base.errors import CoreError, InfrastructureError, error_handler, handled

# ----------------------- #


@error_handler
def _rabbitmq_eh(e: Exception, op: str, **kwargs: Any) -> CoreError:
    match e:
        case CoreError():
            return e

        case aio_pika_errors.AuthenticationError():
            return InfrastructureError("RabbitMQ authentication failed.")

        case aio_pika_errors.IncompatibleProtocolError():
            return InfrastructureError("RabbitMQ protocol mismatch.")

        case aio_pika_errors.AMQPConnectionError():
            return InfrastructureError("RabbitMQ connection error.")

        case aio_pika_errors.ChannelInvalidStateError():
            return InfrastructureError("RabbitMQ channel is in an invalid state.")

        case aio_pika_errors.AMQPChannelError():
            return InfrastructureError("RabbitMQ channel error.")

        case aio_pika_errors.PublishError() | aio_pika_errors.DeliveryError():
            return InfrastructureError("RabbitMQ message delivery failed.")

        case aio_pika_errors.MessageProcessError():
            return InfrastructureError("RabbitMQ message processing failed.")

        case TimeoutError():
            return InfrastructureError("RabbitMQ operation timed out.")

        case _:
            return InfrastructureError(
                f"An error occurred while executing RabbitMQ operation {op}: {e}"
            )


# ----------------------- #

rabbitmq_handled = partial(handled, _rabbitmq_eh)
