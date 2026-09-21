import { query } from "../lib/db.js";
import { redis } from "../lib/redis.js";

// Placeholder — BullMQ worker + HTTP execution logic goes here next.
console.log("Worker starting...");

setInterval(async () => {
  try {
    await query("SELECT 1");
    await redis.ping();
    console.log(`[${new Date().toISOString()}] worker tick — connections ok`);
  } catch (err) {
    console.error("Worker tick failed:", err.message);
  }
}, 5000);
