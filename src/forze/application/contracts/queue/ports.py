from typing import Protocol, runtime_checkable

# ----------------------- #


@runtime_checkable
class QueueReadPort(Protocol): ...


@runtime_checkable
class QueueWritePort(Protocol): ...


@runtime_checkable
class QueueAckPort(Protocol): ...
