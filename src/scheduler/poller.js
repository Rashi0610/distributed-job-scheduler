import { randomUUID } from "crypto";
import { query } from "../lib/db.js";
import { redis } from "../lib/redis.js";
import { claimLock } from "../lib/lock.js";
import { executionQueue } from "../lib/queue.js";

const POLL_INTERVAL_MS = 5000;
const LOCK_TTL_MS = 30000;

const instanceId = randomUUID();

async function findDueJobs() {
  const result = await query(
    `SELECT id, name, next_run_at, target_url, payload,
            cron_expression, run_at, timezone, max_attempts,
            current_occurrence_id
     FROM jobs
     WHERE status = 'active'
       AND next_run_at <= now()
     ORDER BY next_run_at ASC
     LIMIT 20`
  );
  return result.rows;
}

// Decides: is this a brand-new occurrence, or one recovering after an
// interrupted retry chain (e.g. Redis was wiped mid-retry)? If an
// occurrence id already exists on the job, we recount real attempts
// from the durable executions table instead of trusting a fresh "1".
async function resolveOccurrence(job) {
  if (!job.current_occurrence_id) {
    const occurrenceId = randomUUID();
    await query(`UPDATE jobs SET current_occurrence_id = $1 WHERE id = $2`, [occurrenceId, job.id]);
    return { occurrenceId, attempt: 1 };
  }

  // An occurrence id already exists -- this job was already mid-retry
  // at some point. Count real attempts from Postgres rather than
  // trusting anything that might have lived only in Redis.
  const occurrenceId = job.current_occurrence_id;
  const result = await query(
    `SELECT COUNT(*) FROM executions WHERE job_id = $1 AND occurrence_id = $2`,
    [job.id, occurrenceId]
  );
  const priorAttempts = parseInt(result.rows[0].count, 10);
  console.log(`  RECOVERED occurrence ${occurrenceId.slice(0, 8)} for ${job.id} -- ${priorAttempts} prior attempt(s) found in Postgres`);
  return { occurrenceId, attempt: priorAttempts + 1 };
}

async function pollOnce() {
  const dueJobs = await findDueJobs();

  if (dueJobs.length === 0) {
    console.log(`[${new Date().toISOString()}] no due jobs`);
    return;
  }

  console.log(`[${new Date().toISOString()}] found ${dueJobs.length} due job(s), attempting to claim...`);

  for (const job of dueJobs) {
    const claimed = await claimLock(redis, job.id, instanceId, LOCK_TTL_MS);

    if (!claimed) {
      console.log(`  skipped  ${job.id} "${job.name}" -- already claimed by another instance`);
      continue;
    }

    console.log(`  CLAIMED  ${job.id} "${job.name}" by instance ${instanceId.slice(0, 8)}`);

    const { occurrenceId, attempt } = await resolveOccurrence(job);

    await executionQueue.add("execute-job", {
      jobId: job.id,
      name: job.name,
      targetUrl: job.target_url,
      payload: job.payload,
      ownerId: instanceId,
      cronExpression: job.cron_expression,
      runAt: job.run_at,
      oldNextRunAt: job.next_run_at,
      timezone: job.timezone,
      maxAttempts: job.max_attempts,
      occurrenceId,
      attempt,
    });

    console.log(`  enqueued ${job.id} for execution (occurrence ${occurrenceId.slice(0, 8)}, attempt ${attempt})`);
  }
}

async function main() {
  console.log(`Scheduler instance ${instanceId.slice(0, 8)} starting -- polling every ${POLL_INTERVAL_MS / 1000}s`);
  setInterval(() => {
    pollOnce().catch((err) => {
      console.error("Poll failed:", err.message);
    });
  }, POLL_INTERVAL_MS);
}

main();