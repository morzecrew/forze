"""The credential the current unit of work authenticated with, as the verifying plane records it.

A verifier records what it admitted, and a later step of the same request (an issue that must
not outlive the caller's own session) reads it back. Both sides belong to the verifying plane,
which alone knows what the value means. A transport that runs a connection's work in other
tasks carries it: it reads the admission its connection resolver left, and binds it around each
unit of work, as it binds the identity.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

# ----------------------- #

_ADMISSION: ContextVar[object | None] = ContextVar("forze_authn_admission", default=None)


def current_authn_admission() -> object | None:
    """What the current unit of work's credential was admitted as, or ``None``."""

    return _ADMISSION.get()


def record_authn_admission(admission: object | None) -> None:
    """Record what a verification admitted, or ``None`` for a credential with nothing to carry.

    For verifiers: a later verification in the same task replaces it.
    """

    _ADMISSION.set(admission)


@contextmanager
def bind_authn_admission(admission: object | None) -> Iterator[None]:
    """Bind *admission* for the duration, as a transport binds a connection's identity."""

    token = _ADMISSION.set(admission)

    try:
        yield

    finally:
        _ADMISSION.reset(token)
