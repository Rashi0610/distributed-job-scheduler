import parser from "cron-parser";

// Next cron match strictly after `fromDate`. Cron expressions are
// absolute clock times, so the result is always on the cron grid
// (e.g. :00 seconds) no matter what `fromDate` is.
export function nextRunAfter(cronExpression, timezone, fromDate) {
  const interval = parser.parseExpression(cronExpression, {
    currentDate: fromDate,
    tz: timezone || "UTC",
  });
  return interval.next().toDate();
}