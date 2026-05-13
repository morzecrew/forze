from typing import Final

# ----------------------- #

RELEASE_DLOCK: Final = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""

RESET_DLOCK: Final = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
    return 0
end
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
