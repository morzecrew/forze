from typing import Final

# ----------------------- #

ACQUIRE_DLOCK: Final = """
if tonumber(redis.call("GET", KEYS[2]) or "0") >= tonumber(ARGV[3]) then
    return -1
end
if not redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) then
    return 0
end
local token = redis.pcall("INCR", KEYS[2])
if type(token) == "table" and token.err then
    redis.call("DEL", KEYS[1])
end
return token
"""
"""Redis script to acquire a distributed lock and issue a fencing token atomically.

KEYS[1] is the lock key, KEYS[2] the per-key fencing counter. ARGV[1] is the
owner, ARGV[2] the TTL in milliseconds, ARGV[3] the largest token to issue.

On successful ``SET NX PX`` the per-key counter is ``INCR``-ed and its new value
returned (always >= 1); on contention returns 0. A counter already at ARGV[3]
returns -1 before the lock is taken, so no token past it is issued: past 2^53 a
Lua number cannot tell two tokens apart. If the ``INCR`` fails (a counter that is
not an integer), the lock key is deleted again and the error returned, so a
failed acquire never leaves the lock held. The counter deliberately has **no
TTL** so tokens stay monotonically increasing across lock generations even after
the lock key expires — this is the fencing guarantee; the cost is one small
permanent key per lock key. ``RELEASE_DLOCK`` must never delete it.
"""

ADVANCE_DLOCK_FENCE: Final = """
local raw = redis.call('GET', KEYS[1])
if tonumber(raw or '0') < tonumber(ARGV[1]) then
    redis.call('SET', KEYS[1], ARGV[1])
    return ARGV[1]
end
return raw or '0'
"""
"""Redis script to move a distributed lock's fencing counter up to at least a given value.

KEYS[1] is the per-key fencing counter ``ACQUIRE_DLOCK`` increments; ARGV[1] the value, as
a decimal string. Sets the counter to ARGV[1] when it is lower (or absent), never lowers it,
and returns the counter after the call as the stored string. Numbers go in and out as
strings because Lua formats a number above 10^14 with an exponent. The comparison is in
doubles, exact only up to 2^53 - 1, so the caller bounds ARGV[1] to that.
"""

RELEASE_DLOCK: Final = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""
"""Redis script to release a distributed lock.

Execution requires the lock owner to be provided as the first argument.

Returns 1 if the lock was released, 0 if the lock was not owned by the provided owner.
"""

RESET_DLOCK: Final = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
    return 0
end
"""
"""Redis script to reset a distributed lock.

Execution requires the lock owner to be provided as the first argument and the TTL in milliseconds as the second argument.

Returns 1 if the lock was reset, 0 if the lock was not owned by the provided owner.
"""

# Atomic multi-key SET with shared EX/PX and optional NX/XX (all keys succeed or none).
INCR_COUNTER_WITH_TTL: Final = """
local fresh = redis.call('EXISTS', KEYS[1]) == 0
redis.call('INCRBY', KEYS[1], ARGV[1])
if fresh then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return redis.call('GET', KEYS[1])
"""
"""Increment a counter, starting its lifetime only if this call creates it.

KEYS[1] is the counter key. ARGV[1] is the step, ARGV[2] the TTL in milliseconds. A key that
already exists keeps the expiry it has (or none), so a fixed window is never extended by the
traffic inside it — ``EXPIRE ... NX`` would instead put an expiry on a live counter written
without one. Returns the new value as the stored string: a Lua number is a double, which
cannot hold every int64. An ``INCRBY`` that would overflow fails before anything is written.
"""

MSET_BULK_SET: Final = """
local n = #KEYS
local ex = tonumber(ARGV[1])
local px = tonumber(ARGV[2])
local want_nx = tonumber(ARGV[3]) == 1
local want_xx = tonumber(ARGV[4]) == 1

if want_nx and want_xx then
    return redis.error_reply('ERR NX and XX are mutually exclusive')
end

if (#ARGV - 4) ~= n then
    return redis.error_reply('ERR key/value count mismatch')
end

local function set_one(i)
    local k = KEYS[i]
    local v = ARGV[4 + i]
    if want_nx then
        if ex >= 0 and px >= 0 then
            return redis.call('SET', k, v, 'NX', 'EX', ex, 'PX', px)
        elseif ex >= 0 then
            return redis.call('SET', k, v, 'NX', 'EX', ex)
        elseif px >= 0 then
            return redis.call('SET', k, v, 'NX', 'PX', px)
        else
            return redis.call('SET', k, v, 'NX')
        end
    elseif want_xx then
        if ex >= 0 and px >= 0 then
            return redis.call('SET', k, v, 'XX', 'EX', ex, 'PX', px)
        elseif ex >= 0 then
            return redis.call('SET', k, v, 'XX', 'EX', ex)
        elseif px >= 0 then
            return redis.call('SET', k, v, 'XX', 'PX', px)
        else
            return redis.call('SET', k, v, 'XX')
        end
    else
        if ex >= 0 and px >= 0 then
            return redis.call('SET', k, v, 'EX', ex, 'PX', px)
        elseif ex >= 0 then
            return redis.call('SET', k, v, 'EX', ex)
        elseif px >= 0 then
            return redis.call('SET', k, v, 'PX', px)
        else
            return redis.call('SET', k, v)
        end
    end
end

local touched = {}
for i = 1, n do
    local ok = set_one(i)
    if (not ok) then
        for j = 1, #touched do
            redis.call('DEL', touched[j])
        end
        return 0
    end
    table.insert(touched, KEYS[i])
end
return 1
"""
"""Redis script to perform a bulk set operation with shared EX/PX and optional NX/XX (all keys succeed or none).

Execution requires the following arguments:
- ARGV[1]: EX (seconds)
- ARGV[2]: PX (milliseconds)
- ARGV[3]: want_nx (1 or 0)
- ARGV[4]: want_xx (1 or 0)
- ARGV[5]: ...: key/value pairs

Returns 1 if the set operation was successful, 0 if it was not.
"""

# Compare-and-swap append for search snapshot: meta must match ARGV[1] (raw GET bytes).
APPEND_SNAPSHOT_CHUNK: Final = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
local ex = tonumber(ARGV[3])
redis.call('SET', KEYS[2], ARGV[2], 'EX', ex)
redis.call('SET', KEYS[1], ARGV[4], 'EX', ex)
return 1
"""
"""Redis script to compare-and-swap append for search snapshot: meta must match ARGV[1] (raw GET bytes).

Execution requires the following arguments:
- ARGV[1]: meta (raw GET bytes)
- ARGV[2]: chunk (raw bytes)
- ARGV[3]: ex (seconds)
- ARGV[4]: new_meta (raw bytes)

Returns 1 if the append was successful, 0 if the meta did not match.
"""

CIRCUIT_BREAKER_ADMIT: Final = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local break_duration = tonumber(ARGV[4])
local half_open_max = tonumber(ARGV[5])
local ttl_ms = tonumber(ARGV[6])

local data = redis.call('HMGET', KEYS[1],
    'phase', 'window_start', 'successes', 'failures', 'opened_at', 'half_open_calls')
local phase = data[1] or 'closed'
local window_start = tonumber(data[2])
if window_start == nil then window_start = now end
local successes = tonumber(data[3]) or 0
local failures = tonumber(data[4]) or 0
local opened_at = tonumber(data[5]) or 0
local half_open_calls = tonumber(data[6]) or 0

local allowed = 1
local transition = 'none'

if phase == 'open' then
    if now - opened_at >= break_duration then
        phase = 'half_open'
        opened_at = now
        half_open_calls = 0
        transition = 'half_open'
    else
        allowed = 0
    end
end

if allowed == 1 and phase == 'half_open' then
    if now - opened_at >= break_duration then
        opened_at = now
        half_open_calls = 0
    end
    if half_open_calls < half_open_max then
        half_open_calls = half_open_calls + 1
    else
        allowed = 0
    end
end

redis.call('HSET', KEYS[1],
    'phase', phase, 'window_start', window_start, 'successes', successes,
    'failures', failures, 'opened_at', opened_at, 'half_open_calls', half_open_calls)
redis.call('PEXPIRE', KEYS[1], ttl_ms)

return tostring(allowed) .. ':' .. phase .. ':' .. transition
"""
"""Atomic circuit-breaker admit (server-side port of ``BreakerState.try_admit``).

KEYS[1]: breaker state hash. ARGV: failure_ratio, window_s, min_throughput,
break_duration_s, half_open_max_calls, ttl_ms. Uses server ``TIME`` (no clock skew).

Returns ``"<allowed>:<phase>:<transition>"`` — allowed ``1``/``0``, phase
``closed``/``open``/``half_open``, transition ``none``/``half_open``.
"""

CIRCUIT_BREAKER_RECORD: Final = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local ok = ARGV[1] == '1'
local failure_ratio = tonumber(ARGV[2])
local window = tonumber(ARGV[3])
local min_throughput = tonumber(ARGV[4])
local ttl_ms = tonumber(ARGV[7])

local data = redis.call('HMGET', KEYS[1],
    'phase', 'window_start', 'successes', 'failures', 'opened_at', 'half_open_calls')
local phase = data[1] or 'closed'
local window_start = tonumber(data[2])
if window_start == nil then window_start = now end
local successes = tonumber(data[3]) or 0
local failures = tonumber(data[4]) or 0
local opened_at = tonumber(data[5]) or 0
local half_open_calls = tonumber(data[6]) or 0

local transition = 'none'

if ok then
    if phase == 'half_open' then
        phase = 'closed'
        window_start = now
        successes = 0
        failures = 0
        half_open_calls = 0
        transition = 'closed'
    else
        if now - window_start >= window then
            window_start = now
            successes = 0
            failures = 0
        end
        successes = successes + 1
    end
else
    if phase == 'half_open' then
        phase = 'open'
        opened_at = now
        transition = 'open'
    else
        if now - window_start >= window then
            window_start = now
            successes = 0
            failures = 0
        end
        failures = failures + 1
        local total = successes + failures
        if total >= min_throughput and (failures / total) >= failure_ratio then
            phase = 'open'
            opened_at = now
            transition = 'open'
        end
    end
end

redis.call('HSET', KEYS[1],
    'phase', phase, 'window_start', window_start, 'successes', successes,
    'failures', failures, 'opened_at', opened_at, 'half_open_calls', half_open_calls)
redis.call('PEXPIRE', KEYS[1], ttl_ms)

return phase .. ':' .. transition
"""
"""Atomic circuit-breaker record (server-side port of ``BreakerState.on_success`` / ``on_failure``).

KEYS[1]: breaker state hash. ARGV: ok(``1``/``0``), failure_ratio, window_s,
min_throughput, break_duration_s, half_open_max_calls, ttl_ms.

Returns ``"<phase>:<transition>"`` — transition ``none``/``open``/``closed``.
"""

IDEMPOTENCY_COMMIT: Final = """
local cur = redis.call('GET', KEYS[1])
if cur ~= ARGV[1] and cur ~= ARGV[5] then
    return 0
end
local ex = tonumber(ARGV[4])
redis.call('SET', KEYS[2], ARGV[3], 'EX', ex)
redis.call('SET', KEYS[1], ARGV[2], 'EX', ex)
return 1
"""
"""Compare-and-set the idempotency claim to DONE, fenced to the caller's own claim.

KEYS[1] is the metadata key, KEYS[2] the result-body key. ARGV[1] is the exact
serialized *pending* metadata the caller wrote in ``begin``
(``{st:"P",ph:…,own:…}``), ARGV[2] the serialized *done* metadata, ARGV[3] the raw
result body, ARGV[4] the TTL in seconds, ARGV[5] the same pending metadata
**without** the owner.

The write only happens if the current metadata is **byte-for-byte** the caller's
own pending claim: a stale owner whose claim lapsed and was re-acquired by
another writer — a different ``ph``, or the same one under a different ``own`` —
or whose record is already DONE, cannot overwrite it. ARGV[5] is what keeps the
owner additive: a claim taken before the field existed carries no ``own`` and is
still the caller's to complete, which matters for the rolling deploy where one
process wrote the claim and another commits it. Returns 1 on commit, 0 when the
claim is missing, expired, or not the caller's. Byte-exact compare mirrors
``RELEASE_DLOCK`` / ``APPEND_SNAPSHOT_CHUNK`` and avoids parsing JSON server-side.
"""

IDEMPOTENCY_RELEASE: Final = """
local cur = redis.call('GET', KEYS[1])
if cur ~= ARGV[1] and cur ~= ARGV[2] then
    return 0
end
return redis.call('DEL', KEYS[1], KEYS[2])
"""
"""Compare-and-delete a *pending* idempotency claim owned by the caller.

KEYS[1] is the metadata key, KEYS[2] the result-body key. ARGV[1] is the exact
serialized pending metadata the caller wrote in ``begin``, ARGV[2] the same
metadata without the owner (see ``IDEMPOTENCY_COMMIT``). Both keys are deleted
only if the current metadata is byte-for-byte one of those, so a completed record
(DONE) or a claim re-acquired by another writer — a different ``ph``, or the same
one under a different ``own`` — is left untouched. Returns the number of keys
deleted (>= 1) on release, 0 otherwise.
"""

RATE_LIMIT_ACQUIRE: Final = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local ttl_ms = tonumber(ARGV[3])

local data = redis.call('HMGET', KEYS[1], 'tokens', 'updated_at')
local tokens = tonumber(data[1])
local updated_at = tonumber(data[2])

if tokens == nil or updated_at == nil then
    tokens = capacity
    updated_at = now
end

local elapsed = now - updated_at
if elapsed > 0 then
    tokens = math.min(capacity, tokens + elapsed * rate)
end

local allowed = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated_at', now)
redis.call('PEXPIRE', KEYS[1], ttl_ms)

return tostring(allowed)
"""
"""Atomic token-bucket acquire (server-side port of ``RateLimitState.try_acquire``).

KEYS[1]: bucket state hash. ARGV: rate (tokens/second), capacity, ttl_ms. Uses
server ``TIME`` (no clock skew); the bucket starts full on first touch.

Returns ``"1"`` (token consumed) or ``"0"`` (rejected, bucket empty).
"""


# ....................... #


LATENCY_DIGEST_RECORD: Final = """
redis.call('HINCRBY', KEYS[1], ARGV[1], 1)
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
return 'OK'
"""
"""Record one latency sample into a distributed DDSketch (one Redis hash per
``(policy, route)``; field = bucket index, value = count). ``HINCRBY`` merges
samples from every replica into the same bins — the sketch is mergeable by
construction, so the bins reflect the *fleet's* latency distribution.

KEYS[1]: digest hash. ARGV: bucket_index (computed client-side from the shared
``relative_accuracy``), ttl_ms. Returns ``"OK"``.
"""


# ....................... #


LATENCY_DIGEST_QUANTILE: Final = """
local data = redis.call('HGETALL', KEYS[1])
local q = tonumber(ARGV[1])
local min_count = tonumber(ARGV[2])

local indices = {}
local counts = {}
local total = 0

for i = 1, #data, 2 do
    local idx = tonumber(data[i])
    local c = tonumber(data[i + 1])
    indices[#indices + 1] = idx
    counts[idx] = c
    total = total + c
end

if total < min_count then
    return ''
end

table.sort(indices)
local rank = q * (total - 1)
local cumulative = 0

for _, idx in ipairs(indices) do
    cumulative = cumulative + counts[idx]
    if cumulative > rank then
        return tostring(idx)
    end
end

return tostring(indices[#indices])
"""
"""Read the ``q``-quantile from the merged DDSketch hash, walking the bins in
index order to the rank bucket. Returns the bucket index as a string (the client
maps it back to a latency via the same DDSketch bucketing), or ``""`` while the
digest is still warming (fewer than ``min_count`` samples) so the caller holds
the limit rather than acting on a thin estimate.

KEYS[1]: digest hash. ARGV: q (quantile in (0,1)), min_count.
"""


# ....................... #


LATENCY_DIGEST_RESET: Final = """
redis.call('DEL', KEYS[1])
return 'OK'
"""
"""Open a fresh measurement epoch by dropping the digest hash (the old
distribution justified a backoff). Shared across the fleet — any replica's
backoff re-measures the whole fleet together, mirroring the shared breaker.

KEYS[1]: digest hash. Returns ``"OK"``.
"""


# ....................... #


PRESENCE_JOIN: Final = """
local t = redis.call('TIME')
local now_ms = (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000)
local ttl_ms = tonumber(ARGV[2])
redis.call('ZADD', KEYS[1], now_ms + ttl_ms, ARGV[1])
redis.call('PEXPIRE', KEYS[1], ttl_ms + 1000)
return 1
"""
"""Record (or refresh) a connection in a room's presence set, expiring by TTL.

The room is a sorted set of ``sid`` members scored by their absolute expiry
(server ``TIME`` + ttl), so a crashed node's rows lapse on their own. The set
key itself gets a ``PEXPIRE`` just past the newest member so an emptied room
self-cleans. Live connections must re-run this within the TTL (heartbeat).

KEYS[1]: room set. ARGV: sid, ttl_ms. Returns 1.
"""


# ....................... #


PRESENCE_COUNT: Final = """
local t = redis.call('TIME')
local now_ms = (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now_ms)
return redis.call('ZCARD', KEYS[1])
"""
"""Count live connections in a room, pruning lapsed entries first.

Drops members whose expiry score is at or before server ``now`` (the lazy prune
that makes the TTL real even with no writes), then returns the remaining count.

KEYS[1]: room set. Returns the live member count.
"""


# ....................... #


PRESENCE_LEAVE: Final = """
redis.call('ZREM', KEYS[1], ARGV[1])
return redis.call('ZCARD', KEYS[1])
"""
"""Remove a connection from a room's presence set on a clean disconnect.

KEYS[1]: room set. ARGV: sid. Returns the remaining member count.
"""


# ....................... #

_FLEET_NOW: Final = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local now_s = string.format('%d', now)
"""

_FLEET_REPLY: Final = """
local entries = {}
local flat = redis.call('HGETALL', KEYS[2])
for i = 1, #flat, 2 do
    entries[flat[i]] = flat[i + 1]
end
local live = redis.call('ZRANGEBYSCORE', KEYS[1], '(' .. now_s, '+inf', 'WITHSCORES')
local out = {now_s}
for i = 1, #live, 2 do
    local entry = entries[live[i]]
    if entry then
        out[#out + 1] = live[i] .. '\\t' .. live[i + 1] .. '\\t' .. entry
    end
end
return table.concat(out, '\\n')
"""

FLEET_BEAT: Final = (
    _FLEET_NOW
    + """
local id, state, report = ARGV[1], ARGV[2], ARGV[3]

local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now_s)
for i = 1, #expired, 1000 do
    local last = math.min(i + 999, #expired)
    redis.call('ZREM', KEYS[1], unpack(expired, i, last))
    redis.call('HDEL', KEYS[2], unpack(expired, i, last))
end

local since = now_s
local previous = redis.call('HGET', KEYS[2], id)
if previous then
    local cut = string.find(previous, '|', 1, true)
    if cut and string.sub(previous, 1, cut - 1) == state then
        local cut2 = string.find(previous, '|', cut + 1, true)
        local kept = cut2 and string.sub(previous, cut + 1, cut2 - 1)
        if kept and #kept <= 14 and string.find(kept, '^%d+$') then
            since = kept
        end
    end
end

redis.call('HSET', KEYS[2], id, state .. '|' .. since .. '|' .. report)
redis.call('ZADD', KEYS[1], string.format('%d', now + tonumber(ARGV[4])), id)

local top = redis.call('ZRANGE', KEYS[1], -1, -1, 'WITHSCORES')
local keep = string.format('%d', tonumber(top[2]) + 1000)
redis.call('PEXPIREAT', KEYS[1], keep)
redis.call('PEXPIREAT', KEYS[2], keep)
"""
    + _FLEET_REPLY
)
"""Write one member's entry and return the fleet's view, on the server clock.

Removes every expired member from both keys first, in chunks of 1000 (``unpack`` of
thousands of values exceeds Lua's stack), then writes the entry. ``since`` is kept when the
stored entry's state prefix equals the new state, compared as strings, so the report is
never decoded; a stored ``since`` that is not 1 to 14 digits restarts at now. Both keys expire
one second past the **largest** member expiry — never from
this beat's own TTL, so a short final beat cannot expire the fleet under its other members.

KEYS[1]: expiry ZSET, KEYS[2]: entry HASH. ARGV: member id, state, report JSON, ttl ms.
Returns the server time in ms on the first line, then ``id\\texpiry_ms\\tstate|since_ms|report``
per live member.
"""


# ....................... #


FLEET_VIEW: Final = _FLEET_NOW + _FLEET_REPLY
"""Read the fleet's live members without writing, in the same reply shape as ``FLEET_BEAT``.

KEYS[1]: expiry ZSET, KEYS[2]: entry HASH.
"""


# ....................... #


FLEET_LEAVE: Final = """
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
"""
"""Remove one member from both fleet keys at once.

KEYS[1]: expiry ZSET, KEYS[2]: entry HASH. ARGV: member id. Returns 1.
"""


# ....................... #

_REDIS_NOW_MS: Final = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
"""

REVOCATION_READ: Final = (
    _REDIS_NOW_MS
    + """
local epoch = redis.call('GET', KEYS[1])
local state = redis.call('GET', KEYS[2])
return string.format('%d', now)
    .. '\\n' .. (epoch and ('+' .. epoch) or '-')
    .. '\\n' .. (state and ('+' .. state) or '-')
"""
)
"""Read a principal's cached epoch and one state, with the server time, at once.

KEYS[1]: epoch key, KEYS[2]: state key. Returns three lines: the server time in ms, then the
epoch and the state, each ``+<value>`` when present and ``-`` when absent.
"""

REVOCATION_READ_ONE: Final = (
    _REDIS_NOW_MS
    + """
local state = redis.call('GET', KEYS[1])
return string.format('%d', now) .. '\\n' .. (state and ('+' .. state) or '-')
"""
)
"""Read one cached state, with the server time, at once.

KEYS[1]: state key. Returns two lines: the server time in ms, then the state, ``+<value>`` when
present and ``-`` when absent.
"""

REVOCATION_FILL: Final = (
    _REDIS_NOW_MS
    + """
if tonumber(ARGV[1]) <= now then
    return 0
end
for i = 1, #KEYS do
    redis.call('SET', KEYS[i], ARGV[i + 1], 'NX', 'PXAT', ARGV[1])
end
return 1
"""
)
"""Set each absent key, expiring at an instant on the server clock; present keys are kept.

KEYS: state keys. ARGV[1]: expiry in ms since the epoch; ARGV[2..]: one value per key. A call
that runs at or after the expiry writes nothing. Returns 1 when it wrote, else 0.
"""

REVOCATION_RAISE_EPOCH: Final = (
    _REDIS_NOW_MS
    + """
if tonumber(ARGV[2]) <= now then
    return 0
end
local value = ARGV[3]
local cached = redis.call('GET', KEYS[1])
local held = cached and (string.match(cached, '^(%d+)$') or string.match(cached, '^(%d+)|'))
if held and tonumber(held) > tonumber(ARGV[1]) then
    value = cached
end
redis.call('SET', KEYS[1], value, 'PXAT', ARGV[2])
return 1
"""
)
"""Set the cached epoch to the larger of the cached one and ARGV[1], expiring at ARGV[2].

KEYS[1]: epoch key. ARGV[1]: the epoch read from the database, a decimal string; ARGV[2]:
expiry in ms since the epoch; ARGV[3]: the value cached when ARGV[1] wins, the epoch alone or
followed by ``|`` and the caller's fields. A cached value is compared by the decimal integer it
starts with, alone or before a ``|``; one that starts with none is replaced. A call that runs at
or after the expiry writes nothing. Returns 1 when it wrote, else 0.
"""

REVOCATION_NOW: Final = _REDIS_NOW_MS + "return string.format('%d', now)\n"
"""The server time in ms since the epoch.

KEYS[1]: the principal's epoch key, unread; it routes the call to that key's node."""
