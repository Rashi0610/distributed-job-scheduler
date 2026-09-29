-- Inserts N one-time jobs, all already due, targeting httpbin so we
-- get real HTTP round trips. Run this right before starting multiple
-- worker containers.
INSERT INTO jobs (name, target_url, run_at, next_run_at, max_attempts)
SELECT
  'load-test-' || i,
  'https://httpbin.org/post',
  now() - interval '1 minute',
  now() - interval '1 minute',
  3
FROM generate_series(1, 200) AS i;