local key    = KEYS[1]
local now    = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit  = tonumber(ARGV[3])
local member = ARGV[4]

-- Remove entries outside current sliding window
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)

-- Count current entries in window
local count = redis.call('ZCARD', key)

if count >= limit then
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local retry_after = 1
    if #oldest >= 2 then
        local oldest_ts = tonumber(oldest[2])
        retry_after = math.ceil((oldest_ts + window - now) / 1000)
        if retry_after < 1 then retry_after = 1 end
    end
    return {0, retry_after}
end

redis.call('ZADD', key, now, member)
redis.call('PEXPIRE', key, window)

return {1, 0}
