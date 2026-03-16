"""Tests for optional dependency compatibility guards."""

import builtins
from types import ModuleType

import pytest

from forze_rabbitmq._compat import require_rabbitmq
from forze_socketio._compat import require_socketio
from forze_sqs._compat import require_sqs
from forze_temporal._compat import require_temporal


def _mock_import(
    monkeypatch: pytest.MonkeyPatch,
    *,
    module_name: str,
    raises: bool,
) -> None:
    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        if name == module_name:
            if raises:
                raise ImportError(f"No module named '{module_name}'")
            return ModuleType(module_name)

        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)


def test_require_rabbitmq_succeeds_when_module_is_importable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_import(monkeypatch, module_name="aio_pika", raises=False)

    require_rabbitmq()


def test_require_rabbitmq_raises_clear_error_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_import(monkeypatch, module_name="aio_pika", raises=True)

    with pytest.raises(
        RuntimeError, match=r"forze_rabbitmq requires 'forze\[rabbitmq\]' extra"
    ) as exc:
        require_rabbitmq()

    assert isinstance(exc.value.__cause__, ImportError)


def test_require_temporal_succeeds_when_module_is_importable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_import(monkeypatch, module_name="temporalio", raises=False)

    require_temporal()


def test_require_temporal_raises_clear_error_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_import(monkeypatch, module_name="temporalio", raises=True)

    with pytest.raises(
        RuntimeError, match=r"forze_temporal requires 'forze\[temporal\]' extra"
    ) as exc:
        require_temporal()

    assert isinstance(exc.value.__cause__, ImportError)


def test_require_sqs_succeeds_when_module_is_importable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_import(monkeypatch, module_name="aioboto3", raises=False)
    _mock_import(monkeypatch, module_name="aiobotocore", raises=False)
    _mock_import(monkeypatch, module_name="types_aiobotocore_sqs", raises=False)

    require_sqs()


def test_require_sqs_raises_clear_error_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_import(monkeypatch, module_name="aioboto3", raises=True)

    with pytest.raises(
        RuntimeError, match=r"forze_sqs requires 'forze\[sqs\]' extra"
    ) as exc:
        require_sqs()

    assert isinstance(exc.value.__cause__, ImportError)


def test_require_socketio_succeeds_when_module_is_importable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_import(monkeypatch, module_name="socketio", raises=False)

    require_socketio()


def test_require_socketio_raises_clear_error_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_import(monkeypatch, module_name="socketio", raises=True)

    with pytest.raises(
        RuntimeError, match=r"forze_socketio requires 'forze\[socketio\]' extra"
    ) as exc:
        require_socketio()

    assert isinstance(exc.value.__cause__, ImportError)
