"""Unit tests for forze.utils.codecs."""

from forze.utils.codecs import AsciiB64Codec, JsonCodec, KeyCodec, PathCodec, TextCodec

# ----------------------- #


class TestJsonCodec:
    """Tests for JsonCodec."""

    def test_dumps_loads_roundtrip(self) -> None:
        codec = JsonCodec()
        data = {"a": 1, "b": [2, 3]}
        raw = codec.dumps(data)
        assert codec.loads(raw) == data

    def test_dumps_as_str(self) -> None:
        codec = JsonCodec()
        assert codec.dumps_as_str({"x": 1}) == '{"x":1}'


class TestTextCodec:
    """Tests for TextCodec."""

    def test_dumps_loads_roundtrip(self) -> None:
        codec = TextCodec()
        text = "hello"
        assert codec.loads(codec.dumps(text)) == text

    def test_loads_str_passthrough(self) -> None:
        codec = TextCodec()
        assert codec.loads("hello") == "hello"


class TestAsciiB64Codec:
    """Tests for AsciiB64Codec."""

    def test_ascii_passthrough(self) -> None:
        codec = AsciiB64Codec()
        assert codec.dumps("ascii") == "ascii"
        assert codec.loads("ascii") == "ascii"

    def test_unicode_encoded_with_prefix(self) -> None:
        codec = AsciiB64Codec()
        encoded = codec.dumps("café")
        assert encoded.startswith("b64://")
        assert codec.loads(encoded) == "café"


class TestKeyCodec:
    """Tests for KeyCodec."""

    def test_join(self) -> None:
        codec = KeyCodec(namespace="ns")
        assert codec.join("a", "b") == "ns:a:b"

    def test_split(self) -> None:
        codec = KeyCodec(namespace="ns")
        assert codec.split("ns:a:b") == ["ns", "a", "b"]


class TestPathCodec:
    """Tests for PathCodec."""

    def test_join(self) -> None:
        codec = PathCodec()
        assert codec.join("a", "b", "c") == "a/b/c"

    def test_split(self) -> None:
        codec = PathCodec()
        assert codec.split("a/b/c") == ["a", "b", "c"]
