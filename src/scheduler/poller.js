import { randomUUID } from "crypto";
import { query } from "../lib/db.js";
import { redis } from "../lib/redis.js";
import { claimLock } from "../lib/lock.js";
import { executionQueue } from "../lib/queue.js";

const POLL_INTERVAL_MS = 5000;
const LOCK_TTL_MS = 30000;

const instanceId = randomUUID();

async function findDueJobs() {
  // Added cron_expression, run_at, and timezone -- the worker needs
  // these to reschedule the job after it finishes executing.
  const result = await query(
    `SELECT id, name, next_run_at, target_url, payload,
            cron_expression, run_at, timezone, max_attempts
     FROM jobs
     WHERE status = 'active'
       AND next_run_at <= now()
     ORDER BY next_run_at ASC
     LIMIT 20`
  );
  return result.rows;
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

    await executionQueue.add("execute-job", {
      jobId: job.id,
      name: job.name,
      targetUrl: job.target_url,
      payload: job.payload,
      ownerId: instanceId,
      // Needed for rescheduling after execution -- trusting these
      // values as of claim time, per our decision.
      cronExpression: job.cron_expression,
      runAt: job.run_at,
      oldNextRunAt: job.next_run_at,
      timezone: job.timezone,
      maxAttempts: job.max_attempts,
      attempt: 1, // first try -- the worker increments this on each retry
    });

    console.log(`  enqueued ${job.id} for execution (lock stays held until worker finishes)`);
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