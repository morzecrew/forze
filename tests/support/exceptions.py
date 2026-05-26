"""Test helpers for :class:`~forze.base.exceptions.CoreException` assertions."""

from __future__ import annotations

from forze.base.exceptions import CoreException, ExceptionKind
from typing import TypeAlias

import pytest


# ----------------------- #

ExcInfo: TypeAlias = pytest.ExceptionInfo[CoreException]

# ....................... #


def assert_kind(err: BaseException, kind: ExceptionKind) -> CoreException:
    """Assert *err* is a :class:`CoreException` of *kind*."""

    assert isinstance(err, CoreException)
    assert err.kind is kind
    return err


def assert_not_found(err: BaseException) -> CoreException:
    return assert_kind(err, ExceptionKind.NOT_FOUND)


def assert_infrastructure(err: BaseException) -> CoreException:
    return assert_kind(err, ExceptionKind.INFRASTRUCTURE)


def assert_authentication(err: BaseException) -> CoreException:
    return assert_kind(err, ExceptionKind.AUTHENTICATION)


def assert_validation(err: BaseException) -> CoreException:
    return assert_kind(err, ExceptionKind.VALIDATION)


def assert_precondition(err: BaseException) -> CoreException:
    return assert_kind(err, ExceptionKind.PRECONDITION)


def assert_conflict(err: BaseException) -> CoreException:
    return assert_kind(err, ExceptionKind.CONFLICT)


def assert_concurrency(err: BaseException) -> CoreException:
    return assert_kind(err, ExceptionKind.CONCURRENCY)
