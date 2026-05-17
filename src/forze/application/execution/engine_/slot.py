from __future__ import annotations

from enum import StrEnum, auto

# ----------------------- #


class MiddlewareSlot(StrEnum):
    """Slot for a middleware in a middleware plan."""

    # Outside a transaction

    before = auto()
    wrap = auto()
    on_success = auto()
    on_failure = auto()
    finally_ = auto()

    # Inside a transaction

    tx_before = auto()
    tx_wrap = auto()
    tx_on_success = auto()
    tx_on_failure = auto()
    tx_finally = auto()

    # After successful commit

    after_commit = auto()

    # ....................... #

    @classmethod
    def _schedulable(cls) -> list[MiddlewareSlot]:
        return [
            cls.before,
            cls.on_success,
            cls.tx_before,
            cls.tx_on_success,
            cls.after_commit,
        ]

    # ....................... #

    def is_schedulable(self) -> bool:
        return self in self._schedulable()

    # ....................... #

    def is_before(self) -> bool:
        return self in (MiddlewareSlot.before, MiddlewareSlot.tx_before)

    # ....................... #

    @classmethod
    def iter_slot_order(cls) -> list[MiddlewareSlot]:
        return [
            cls.before,
            cls.wrap,
            cls.finally_,
            cls.on_failure,
            cls.tx_before,
            cls.tx_finally,
            cls.tx_on_failure,
            cls.tx_wrap,
            cls.tx_on_success,
            cls.after_commit,
            cls.on_success,
        ]

    # ....................... #

    def requires_tx(self) -> bool:
        return self in (
            MiddlewareSlot.tx_before,
            MiddlewareSlot.tx_wrap,
            MiddlewareSlot.tx_finally,
            MiddlewareSlot.tx_on_failure,
            MiddlewareSlot.tx_on_success,
        )
