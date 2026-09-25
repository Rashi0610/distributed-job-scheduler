import Redis from "ioredis";
import "dotenv/config";

const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

// BullMQ needs its own dedicated connections (it uses blocking Redis
// commands internally) -- so instead of sharing one client everywhere,
// this factory lets each thing that needs a connection make its own.
export function createConnection() {
  const client = new Redis(REDIS_URL, { maxRetriesPerRequest: null });
  client.on("error", (err) => console.error("Redis connection error:", err));
  return client;
}

// A single shared connection for simple, non-blocking stuff -- our
// lock claim/release calls (SET, EVAL). This one's fine to share.
export const redis = createConnection();