from typing import Protocol, runtime_checkable

# ----------------------- #


@runtime_checkable
class PubSubPublishPort(Protocol): ...


@runtime_checkable
class PubSubSubscribePort(Protocol): ...
