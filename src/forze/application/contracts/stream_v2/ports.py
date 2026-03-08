from typing import Protocol, runtime_checkable

# ----------------------- #


@runtime_checkable
class StreamReadPort(Protocol): ...


@runtime_checkable
class StreamWritePort(Protocol): ...


@runtime_checkable
class StreamGroupReadPort(Protocol): ...


@runtime_checkable
class StreamMaintenancePort(Protocol): ...
