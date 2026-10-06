-- KEYS[1] = rt:{sid}
-- ARGV[1] = hash of the presented secret
-- ARGV[2] = hash of the new secret
-- ARGV[3] = jti that uses the new secret
-- returns {status, old_jti, sub, scope}
local cur = redis.call('HMGET', KEYS[1], 'hash', 'jti', 'sub', 'scope')
if not cur[1] then
  return {0, '', '', ''}              -- missing: expired or revoked
end
if cur[1] ~= ARGV[1] then
  redis.call('DEL', KEYS[1])          -- reuse: kill the session so #2 can't be used
  return {-1, cur[2], '', ''}         -- mismatch: stale secret, publish cur[2]
end
redis.call('HSET', KEYS[1], 'hash', ARGV[2], 'jti', ARGV[3])
return {1, cur[2], cur[3], cur[4]}    -- rotated, revoke cur[2]