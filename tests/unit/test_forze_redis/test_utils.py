import sys
from unittest.mock import MagicMock

# Mock 'redis' before it's imported via 'forze_redis', then restore the original
# entries so this module does not pollute ``sys.modules`` for tests that need the
# real ``redis`` package (notably the integration tests that exercise
# ``isinstance(client, Pipeline)`` inside ``redis-py`` itself).
_MOCKED_REDIS_MODULES = (
    "redis",
    "redis.asyncio",
    "redis.asyncio.client",
    "redis.asyncio.connection",
)
_saved_redis_modules = {
    name: sys.modules.get(name) for name in _MOCKED_REDIS_MODULES
}
for _name in _MOCKED_REDIS_MODULES:
    sys.modules[_name] = MagicMock()

try:
    from forze_redis.kernel.platform.utils import parse_stream_entries
finally:
    for _name, _original in _saved_redis_modules.items():
        if _original is None:
            sys.modules.pop(_name, None)
        else:
            sys.modules[_name] = _original

def test_parse_stream_entries_empty() -> None:
    assert parse_stream_entries(None) == []
    assert parse_stream_entries([]) == []

def test_parse_stream_entries_basic() -> None:
    raw = [
        (b"stream1", [(b"1-0", {b"k1": b"v1"})])
    ]
    expected = [
        ("stream1", [("1-0", {b"k1": b"v1"})])
    ]
    assert parse_stream_entries(raw) == expected

def test_parse_stream_entries_mixed_types() -> None:
    raw = [
        ("stream1", [("1-0", {"k1": "v1"})])
    ]
    expected = [
        ("stream1", [("1-0", {b"k1": b"v1"})])
    ]
    assert parse_stream_entries(raw) == expected

def test_parse_stream_entries_list_data() -> None:
    # redis-py can return data as a list of tuples
    raw = [
        (b"stream1", [(b"1-0", [(b"k1", b"v1")])])
    ]
    expected = [
        ("stream1", [("1-0", {b"k1": b"v1"})])
    ]
    assert parse_stream_entries(raw) == expected

def test_parse_stream_entries_multiple() -> None:
    raw = [
        (b"s1", [(b"1-0", {b"a": b"1"}), (b"2-0", {b"b": b"2"})]),
        (b"s2", [(b"3-0", {b"c": b"3"})])
    ]
    expected = [
        ("s1", [("1-0", {b"a": b"1"}), ("2-0", {b"b": b"2"})]),
        ("s2", [("3-0", {b"c": b"3"})])
    ]
    assert parse_stream_entries(raw) == expected

if __name__ == "__main__":
    test_parse_stream_entries_empty()
    test_parse_stream_entries_basic()
    test_parse_stream_entries_mixed_types()
    test_parse_stream_entries_list_data()
    test_parse_stream_entries_multiple()
    print("All tests passed!")
