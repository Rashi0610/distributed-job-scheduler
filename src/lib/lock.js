// Shared lock logic -- used by both the scheduler (to claim) and the
// worker (to release, once execution finishes). Kept in one place so
// the claim/release contract can't drift out of sync between the two.

const RELEASE_LOCK_SCRIPT = `
  if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
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