import { Queue } from "bullmq";
import { createConnection } from "./redis.js";

// One queue, one job type for now ("execute-job"). The worker (next
// step) will listen on this same queue name.
export const executionQueue = new Queue("job-execution", {
  connection: createConnection(),
}); 