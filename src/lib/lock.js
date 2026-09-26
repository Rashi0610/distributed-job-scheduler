// Shared lock logic -- used by the scheduler (to claim), the worker
// (to release, or to extend during a retry chain).

const RELEASE_LOCK_SCRIPT = `
  if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
  else
    return 0
  end
`;

// Same ownership check as release, but extends the TTL instead of
// deleting. This is what lets a lock survive a multi-attempt retry
// chain without needing to guess a total duration up front -- each
// retry just pushes the expiry a bit further, only as far as it
// currently needs to reach.
const EXTEND_LOCK_SCRIPT = `
  if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
  else
    return 0
  end
`;

function lockKeyFor(jobId) {
  return `job-lock:${jobId}`;
}

export async function claimLock(redisClient, jobId, ownerId, ttlMs) {
  const result = await redisClient.set(lockKeyFor(jobId), ownerId, "NX", "PX", ttlMs);
  return result === "OK";
}

export async function releaseLock(redisClient, jobId, ownerId) {
  const result = await redisClient.eval(RELEASE_LOCK_SCRIPT, 1, lockKeyFor(jobId), ownerId);
  return result === 1;
}

export async function extendLock(redisClient, jobId, ownerId, ttlMs) {
  const result = await redisClient.eval(EXTEND_LOCK_SCRIPT, 1, lockKeyFor(jobId), ownerId, ttlMs);
  return result === 1;
}