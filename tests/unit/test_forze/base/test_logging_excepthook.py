"""Tests for excepthook installation helpers."""

import sys

import pytest

import forze.base.logging.excepthook as excepthook_module


@pytest.fixture(autouse=True)
def _restore_excepthook() -> None:
    original = sys.excepthook
    old_hook = excepthook_module.__dict__.get("__old_excepthook")
    yield
    sys.excepthook = original
    excepthook_module.__dict__["__old_excepthook"] = old_hook


def test_install_logs_uncaught_exception(mocker) -> None:
    logger = mocker.Mock()
    mocker.patch.object(excepthook_module, "uncaught_logger", logger)

    excepthook_module.install_excepthook(call_previous=False)

    exc = RuntimeError("boom")
    sys.excepthook(type(exc), exc, None)

    logger.critical.assert_called_once()
    call_kwargs = logger.critical.call_args.kwargs
    assert call_kwargs["exc_info"][0] is RuntimeError
    assert call_kwargs["exc_info"][1] is exc


def test_keyboard_interrupt_calls_previous_hook_only(mocker) -> None:
    previous_hook = mocker.Mock()
    logger = mocker.Mock()
    mocker.patch.object(excepthook_module, "uncaught_logger", logger)
    sys.excepthook = previous_hook

    excepthook_module.install_excepthook(call_previous=False)

    interrupt = KeyboardInterrupt()
    sys.excepthook(KeyboardInterrupt, interrupt, None)

    previous_hook.assert_called_once_with(KeyboardInterrupt, interrupt, None)
    logger.critical.assert_not_called()


def test_call_previous_true_calls_previous_after_logging(mocker) -> None:
    previous_hook = mocker.Mock()
    logger = mocker.Mock()
    mocker.patch.object(excepthook_module, "uncaught_logger", logger)
    sys.excepthook = previous_hook

    excepthook_module.install_excepthook(call_previous=True)

    exc = ValueError("bad")
    sys.excepthook(ValueError, exc, None)

    logger.critical.assert_called_once()
    previous_hook.assert_called_once_with(ValueError, exc, None)


def test_uninstall_restores_original_hook(mocker) -> None:
    original_hook = mocker.Mock()
    sys.excepthook = original_hook

    excepthook_module.install_excepthook()
    installed_hook = sys.excepthook
    assert installed_hook is not original_hook

    excepthook_module.uninstall_excepthook()

    assert sys.excepthook is original_hook
    assert excepthook_module.__dict__["__old_excepthook"] is None
