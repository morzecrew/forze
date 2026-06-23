from typing import Final

# ----------------------- #

ACQUIRE_DLOCK: Final = """
if redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) then
    return redis.call("INCR", KEYS[2])
else
    return 0
end
"""
"""Redis script to acquire a distributed lock and issue a fencing token atomically.

KEYS[1] is the lock key, KEYS[2] the per-key fencing counter. ARGV[1] is the
owner, ARGV[2] the TTL in milliseconds.

On successful ``SET NX PX`` the per-key counter is ``INCR``-ed and its new value
returned (always >= 1); on contention returns 0. The counter deliberately has
**no TTL** so tokens stay monotonically increasing across lock generations even
after the lock key expires — this is the fencing guarantee; the cost is one
small permanent key per lock key. ``RELEASE_DLOCK`` must never delete it.
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
        half_open_calls = 0
        transition = 'half_open'
    else
        allowed = 0
    end
end

if allowed == 1 and phase == 'half_open' then
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
