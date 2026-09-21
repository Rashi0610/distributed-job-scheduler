import { query } from "../lib/db.js";
import { redis } from "../lib/redis.js";

// Placeholder — the actual polling loop and Redis lock logic goes here
// in the next step. For now this just proves the container boots and
// can talk to both Postgres and Redis.
console.log("Scheduler starting...");

setInterval(async () => {
  try {
    await query("SELECT 1");
    await redis.ping();
    console.log(`[${new Date().toISOString()}] scheduler tick — connections ok`);
  } catch (err) {
    console.error("Scheduler tick failed:", err.message);
  }
}, 5000);
