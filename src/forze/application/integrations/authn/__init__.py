"""Shared authn integration helpers (port composition over the authn contracts)."""

from .lockout import (
    LOCKED_LOGIN_CODE,
    LOCKED_LOGIN_MSG,
    LOCKOUT_COUNTER_ROUTE,
    LockoutConfig,
    LoginLockoutGuard,
)
from .orchestrator import AuthnOrchestrator
from .sinks import LoggingAuthnEventSink

__all__ = [
    "AuthnOrchestrator",
    "LOCKED_LOGIN_CODE",
    "LOCKED_LOGIN_MSG",
    "LOCKOUT_COUNTER_ROUTE",
    "LockoutConfig",
    "LoggingAuthnEventSink",
    "LoginLockoutGuard",
]
