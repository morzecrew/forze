"""Tests for forze.application.contracts.actor."""

from uuid import UUID, uuid4

from forze.application.contracts.actor import ActorContextPort


class _StubActorContext:
    """Concrete implementation for testing the ActorContextPort protocol."""

    def __init__(self) -> None:
        self._actor_id: UUID | None = None

    def get(self) -> UUID:
        if self._actor_id is None:
            raise RuntimeError("No actor bound")
        return self._actor_id

    def set(self, actor_id: UUID) -> None:
        self._actor_id = actor_id


class TestActorContextPort:
    def test_is_runtime_checkable(self) -> None:
        stub = _StubActorContext()
        assert isinstance(stub, ActorContextPort)

    def test_set_and_get(self) -> None:
        stub = _StubActorContext()
        uid = uuid4()
        stub.set(uid)
        assert stub.get() == uid

    def test_get_without_set_raises(self) -> None:
        stub = _StubActorContext()
        try:
            stub.get()
            assert False, "Should have raised"
        except RuntimeError:
            pass

    def test_non_conforming_object_not_instance(self) -> None:
        class Bad:
            pass

        assert not isinstance(Bad(), ActorContextPort)
