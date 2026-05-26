"""Authn operation catalog for transport attach (protocol-agnostic)."""

from dataclasses import dataclass
from typing import Final

from forze.application.composition.authn.operations import AuthnKernelOp
from forze.base.primitives import StrKey

# ----------------------- #
#! Super useless isn't it?


@dataclass(frozen=True, slots=True)
class AuthnOperationEntry:
    """One attachable authn operation."""

    enable_name: str
    facade_attr: str
    kernel_op: StrKey


class AuthnPreset:
    """Default authn operations for ``enable=``."""

    ALL: Final = ("password_login", "refresh", "logout", "change_password")


AUTHN_OPERATIONS: dict[str, AuthnOperationEntry] = {
    "password_login": AuthnOperationEntry(
        "password_login",
        "password_login",
        AuthnKernelOp.PASSWORD_LOGIN,
    ),
    "refresh": AuthnOperationEntry(
        "refresh",
        "refresh_tokens",
        AuthnKernelOp.REFRESH_TOKENS,
    ),
    "logout": AuthnOperationEntry("logout", "logout", AuthnKernelOp.LOGOUT),
    "change_password": AuthnOperationEntry(
        "change_password",
        "change_password",
        AuthnKernelOp.CHANGE_PASSWORD,
    ),
}
