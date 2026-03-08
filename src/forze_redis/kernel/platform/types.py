from typing import Mapping, Optional, Sequence

# ----------------------- #

RedisStreamFields = dict[bytes, bytes]
RedisStreamEntry = tuple[str, RedisStreamFields]
RedisStreamBatch = tuple[str, list[RedisStreamEntry]]
RedisStreamResponse = list[RedisStreamBatch]
RawRedisStreamResponse = Optional[
    Sequence[
        tuple[
            str | bytes,
            list[tuple[str | bytes, object]],
        ]
    ]
]

# ....................... #

RedisPubSubMessage = tuple[str, bytes]
RawRedisPubSubMessage = Mapping[str, object]
