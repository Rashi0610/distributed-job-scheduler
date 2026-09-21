import pg from "pg";
import "dotenv/config";

const { Pool } = pg;

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

pool.on("error", (err) => {
  // A background client failed — log it, don't crash the process on
  // every transient connection error.
  console.error("Unexpected Postgres pool error:", err);
});

export async function query(text, params) {
  return pool.query(text, params);
}
