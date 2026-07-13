from forze_kafka._compat import require_kafka

require_kafka()

# ....................... #

from collections.abc import Mapping
from typing import Any

from aiokafka import errors as kafka_errors

from forze.base.conformity import static_fn_conformity
from forze.base.exceptions import (
    CoreException,
    ExceptionMapper,
    build_exc_interceptor,
)

# ----------------------- #


@static_fn_conformity(ExceptionMapper)  # type: ignore[type-abstract]
def _kafka_eh(
    exc: BaseException,
    *,
    site: str,
    details: Mapping[str, Any] | None = None,
) -> CoreException | None:
    _ = site

    match exc:
        case (
            kafka_errors.TopicAuthorizationFailedError()
            | kafka_errors.GroupAuthorizationFailedError()
        ):
            return CoreException.infrastructure(
                "Kafka authorization failed.",
                details=details,
            )

        case kafka_errors.KafkaConnectionError():
            return CoreException.infrastructure(
                "Kafka connection error.",
                details=details,
            )

        case kafka_errors.NodeNotReadyError() | kafka_errors.GroupCoordinatorNotAvailableError():
            return CoreException.infrastructure(
                "Kafka broker / coordinator not ready.",
                details=details,
            )

        case kafka_errors.ProducerClosed() | kafka_errors.ConsumerStoppedError():
            return CoreException.infrastructure(
                "Kafka client is closed.",
                details=details,
            )

        case kafka_errors.KafkaTimeoutError() | TimeoutError():
            return CoreException.infrastructure(
                "Kafka operation timed out.",
                details=details,
            )

        case kafka_errors.KafkaError():
            return CoreException.infrastructure(
                "Kafka error.",
                details=details,
            )

        case _:
            return None


# ....................... #

exc_interceptor = build_exc_interceptor("Kafka", _kafka_eh)
