import { Worker } from "bullmq";
import parser from "cron-parser";
import { createConnection, redis } from "../lib/redis.js";
import { releaseLock } from "../lib/lock.js";
import { query } from "../lib/db.js";

// Decides what happens to the job's schedule after this execution.
// Anchors recurring jobs to the OLD next_run_at (not "now") so small
// delays in individual runs don't accumulate into long-term drift.
async function reschedule(job) {
  if (job.cronExpression) {
    const interval = parser.parseExpression(job.cronExpression, {
      currentDate: new Date(job.oldNextRunAt),
      tz: job.timezone || "UTC",
    });
    const nextRunAt = interval.next().toDate();

    await query(`UPDATE jobs SET next_run_at = $1, updated_at = now() WHERE id = $2`, [
      nextRunAt,
      job.jobId,
    ]);

    console.log(`  rescheduled ${job.jobId} -- next run at ${nextRunAt.toISOString()}`);
  } else {
    // One-time job (run_at was set, no cron_expression) -- it already
    // ran its one and only occurrence. Archive it so it stops showing
    // up as "due".
    await query(`UPDATE jobs SET status = 'archived', updated_at = now() WHERE id = $1`, [job.jobId]);
    console.log(`  archived ${job.jobId} -- one-time job, already ran`);
  }
}

async function executeJob(bullJob) {
  const job = bullJob.data;
  const startedAt = new Date();

  console.log(`  EXECUTING ${job.jobId} "${job.name}" -> ${job.targetUrl}`);

  let status;
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
    status = response.ok ? "success" : "failed";
  } catch (err) {
    if (err.name === "AbortError") {
      status = "timeout";
      errorMessage = "Request timed out";
    } else {
      status = "failed";
      errorMessage = err.message;
    }
  }

  const finishedAt = new Date();

  await query(
    `INSERT INTO executions
       (job_id, scheduled_for, status, claimed_by, started_at, finished_at, response_status, error_message)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
    [job.jobId, job.oldNextRunAt, status, job.ownerId, startedAt, finishedAt, responseStatus, errorMessage]
  );

  // Reschedule BEFORE releasing the lock -- this ordering matters.
  // If we released the lock first, another instance could claim and
  // re-execute this job in the gap before next_run_at is updated.
  // Updating first closes that window entirely.
  await reschedule(job);

  const released = await releaseLock(redis, job.jobId, job.ownerId);

  console.log(
    `  ${status.toUpperCase()} ${job.jobId} -- lock released: ${released}` +
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