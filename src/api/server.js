import express from "express";
import { query } from "../lib/db.js";
import { redis } from "../lib/redis.js";

const app = express();
app.use(express.json());

// Proves the whole chain (app -> Postgres, app -> Redis) is actually wired up.
// This is the first thing to run after `docker compose up` — if this
// doesn't return healthy, nothing else is worth debugging yet.
app.get("/health", async (req, res) => {
  const checks = { postgres: false, redis: false };

  try {
    await query("SELECT 1");
    checks.postgres = true;
  } catch (err) {
    checks.postgres = err.message;
  }

  try {
    await redis.ping();
    checks.redis = true;
  } catch (err) {
    checks.redis = err.message;
  }

  const healthy = checks.postgres === true && checks.redis === true;
  res.status(healthy ? 200 : 503).json({ healthy, checks });
});

// Register a new job. Real validation and cron-expression parsing
// comes in the next step — this just proves the write path works.
app.post("/jobs", async (req, res) => {
  const { name, target_url, cron_expression, run_at, payload } = req.body;

  if (!name || !target_url || (!cron_expression && !run_at)) {
    return res.status(400).json({
      error: "name, target_url, and one of cron_expression or run_at are required",
    });
  }

  const nextRunAt = run_at || new Date(); // placeholder — cron parsing comes next

  const result = await query(
    `INSERT INTO jobs (name, target_url, cron_expression, run_at, payload, next_run_at)
     VALUES ($1, $2, $3, $4, $5, $6)
     RETURNING *`,
    [name, target_url, cron_expression || null, run_at || null, payload || {}, nextRunAt]
  );

  res.status(201).json(result.rows[0]);
});

app.get("/jobs", async (req, res) => {
  const result = await query("SELECT * FROM jobs ORDER BY created_at DESC LIMIT 50");
  res.json(result.rows);
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`API listening on port ${port}`);
});
