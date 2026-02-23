from typing import Any, Optional, Sequence

from .types import StreamMessage

# ----------------------- #
#! very questionable stuff below

KeyT = str | bytes
MessageT = tuple[KeyT, Any]
# Redis XREAD returns None on block timeout, else list of (stream_name, [(id, {k:v}), ...])
RawStreamResponseT = Optional[Sequence[tuple[KeyT, list[MessageT]]]]

# ....................... #


def parse_stream_messages(
    raw: RawStreamResponseT,
) -> list[tuple[str, list[StreamMessage]]]:
    if raw is None or not raw:
        return []

    out: list[tuple[str, list[tuple[str, dict[bytes, bytes]]]]] = []

    for sn_raw, messages in raw:
        sn = (
            sn_raw.decode("utf-8")
            if isinstance(sn_raw, (bytes, bytearray))
            else str(sn_raw)
        )
        parsed_messages: list[tuple[str, dict[bytes, bytes]]] = []

        for m_raw, data_raw in messages:  # type: ignore[reportUnknownArgumentType]
            msg_id = (
                m_raw.decode("utf-8")
                if isinstance(m_raw, (bytes, bytearray))
                else str(m_raw)  # type: ignore[reportUnknownArgumentType]
            )

            if isinstance(data_raw, dict):
                data_dict = data_raw  # type: ignore[reportUnknownReturnType]

            else:
                data_dict = dict(data_raw)  # type: ignore[reportUnknownReturnType]

            parsed_messages.append((msg_id, data_dict))  # type: ignore[reportUnknownArgumentType]

        out.append((sn, parsed_messages))

    return out
