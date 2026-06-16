"""Decrypt raw persistence rows once, ahead of any decode path."""

from typing import Any

from forze.base.primitives import JsonDict
from forze.base.serialization import ModelCodec

# ----------------------- #


async def decrypt_rows(
    codec: ModelCodec[Any, Any], rows: list[JsonDict]
) -> tuple[list[JsonDict], ModelCodec[Any, Any]]:
    """Decrypt sealed fields in raw DB rows **once**, returning the rows and the codec to
    decode them with.

    Field encryption seals values at rest, and every read path — a spec model, a custom
    ``return_type``, or a raw field projection — must see plaintext. Decryption belongs to the
    *row*, not to one decode path, so this runs the encrypting codec's async warm pass and its
    synchronous per-row decrypt once, then hands back the **plain inner** codec (decoding with
    the encrypting codec would re-attempt the now-undone decryption). When *codec* is not an
    encrypting one this is a no-op: the rows and codec are returned unchanged.
    """

    decrypt_mapping = getattr(codec, "decrypt_mapping", None)
    if decrypt_mapping is None:
        return rows, codec

    prepare_decrypt = getattr(codec, "prepare_decrypt", None)
    if prepare_decrypt is not None:
        await prepare_decrypt(rows)

    decrypted = [decrypt_mapping(dict(row)) for row in rows]
    return decrypted, getattr(codec, "inner", codec)
