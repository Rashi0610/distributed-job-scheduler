import { Worker } from "bullmq";
import { createConnection, redis } from "../lib/redis.js";
import { releaseLock, extendLock } from "../lib/lock.js";
import { query } from "../lib/db.js";
import { executionQueue } from "../lib/queue.js";
import { nextRunAfter } from "../lib/schedule.js";

// Decides the job's schedule after an occurrence finishes: next cron
// time for recurring jobs, archive for one-time jobs.
async function reschedule(job) {
  if (job.cronExpression) {
    // Normally anchor to the old scheduled time, so a late run does not
    // skip occurrences. After a fire_once misfire, anchor to now instead
    // to jump past everything that was missed.
    const from = job.rescheduleFromNow ? new Date() : new Date(job.oldNextRunAt);
    const nextRunAt = nextRunAfter(job.cronExpression, job.timezone, from);
    await query(
      `UPDATE jobs SET next_run_at = $1, current_occurrence_id = NULL, updated_at = now() WHERE id = $2`,
      [nextRunAt, job.jobId]
    );
    console.log(`  rescheduled ${job.jobId} -- next run at ${nextRunAt.toISOString()}`);
  } else {
    await query(
      `UPDATE jobs SET status = 'archived', current_occurrence_id = NULL, updated_at = now() WHERE id = $1`,
      [job.jobId]
    );
    console.log(`  archived ${job.jobId} -- one-time job, already ran`);
  }
}

// attempt 1 -> 2s, attempt 2 -> 4s, attempt 3 -> 8s ...
function backoffMs(attempt) {
  return Math.pow(2, attempt) * 1000;
}

async function executeJob(bullJob) {
  const job = bullJob.data;
  const startedAt = new Date();

  // Claim this exact (occurrence, attempt) pair before doing anything
  // else. If a redelivered message (e.g. BullMQ's own stall recovery
  // after a killed worker) arrives for an attempt we already claimed,
  // the unique constraint makes Postgres reject the second INSERT --
  // atomically, so there is no race window. The loser exits here,
  // before ever calling the target URL.
  const claim = await query(
    `INSERT INTO executions (job_id, occurrence_id, scheduled_for, attempt, status, claimed_by, started_at)
     VALUES ($1, $2, $3, $4, 'running', $5, $6)
     ON CONFLICT (occurrence_id, attempt) DO NOTHING
     RETURNING id`,
    [job.jobId, job.occurrenceId, job.oldNextRunAt, job.attempt, job.ownerId, startedAt]
  );

  if (claim.rowCount === 0) {
    console.log(`  DUPLICATE DELIVERY ${job.jobId} occurrence ${job.occurrenceId} attempt ${job.attempt} -- already claimed, skipping (no HTTP call made)`);
    return;
  }
  const executionId = claim.rows[0].id;

  console.log(`  EXECUTING ${job.jobId} "${job.name}" (attempt ${job.attempt}/${job.maxAttempts}) -> ${job.targetUrl}`);

  let rawStatus;
  let responseStatus = null;
  let errorMessage = null;

  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 10000);

    const response = await fetch(job.targetUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(job.payload || {}),
      signal: controller.signal,
    });

    clearTimeout(timeoutId);
    responseStatus = response.status;
    rawStatus = response.ok ? "success" : "failed";
  } catch (err) {
    if (err.name === "AbortError") {
      rawStatus = "timeout";
      errorMessage = "Request timed out";
    } else {
      rawStatus = "failed";
      errorMessage = err.message;
    }
  }

  const finishedAt = new Date();
  const failed = rawStatus !== "success";
  const exhausted = failed && job.attempt >= job.maxAttempts;

  // If this failed attempt used up the last retry, record it as
  // dead_letter (a distinct, queryable terminal state) rather than
  // just "failed" -- makes it easy to later ask "show me everything
  // that gave up."
  const recordedStatus = exhausted ? "dead_letter" : rawStatus;

  // Update the row we claimed above with the real outcome, rather
  // than inserting a second row.
  await query(
    `UPDATE executions
     SET status = $1, finished_at = $2, response_status = $3, error_message = $4
     WHERE id = $5`,
    [recordedStatus, finishedAt, responseStatus, errorMessage, executionId]
  );

  if (failed && !exhausted) {
    // Retry path: still holding the lock, still not done with this
    // occurrence. Calculate backoff, extend the lock to cover the
    // wait, update next_run_at so the scheduler's own query stays
    // consistent, and re-add a new message for the next attempt.
    const delayMs = backoffMs(job.attempt);
    const retryAt = new Date(Date.now() + delayMs);

    // Extend enough to cover the wait plus a buffer for the next
    // attempt's own execution time.
    const extended = await extendLock(redis, job.jobId, job.ownerId, delayMs + 15000);

    await query(`UPDATE jobs SET next_run_at = $1, updated_at = now() WHERE id = $2`, [retryAt, job.jobId]);

    await executionQueue.add(
      "execute-job",
      { ...job, attempt: job.attempt + 1 },
      { delay: delayMs }
    );

    console.log(
      `  RETRY ${job.jobId} -- attempt ${job.attempt} failed, retrying in ${delayMs / 1000}s ` +
        `(lock extended: ${extended})`
    );
    return; // do NOT release the lock -- the retry chain still owns it
  }

  if (exhausted) {
    console.log(`  ALERT: ${job.jobId} "${job.name}" exhausted all ${job.maxAttempts} attempts -- dead-lettered`);
  }

  // Either success, or exhausted (give up on THIS occurrence but the
  // job itself carries on to its next scheduled occurrence).
  await reschedule(job);

  const released = await releaseLock(redis, job.jobId, job.ownerId);

  console.log(
    `  ${recordedStatus.toUpperCase()} ${job.jobId} -- lock released: ${released}` +
      (responseStatus ? ` (HTTP ${responseStatus})` : errorMessage ? ` (${errorMessage})` : "")
  );
}

const worker = new Worker("job-execution", executeJob, {
  connection: createConnection(),
});

worker.on("failed", (bullJob, err) => {
  console.error(`Unexpected error processing job ${bullJob?.id}:`, err.message);
});

console.log("Worker started -- listening for jobs on 'job-execution' queue");