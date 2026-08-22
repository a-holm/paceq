package logsink

// This package owns the log files of step attempts: writing them under a
// quota, and reading them back for the logs command.
//
// The rules it exists to hold:
//
//   - Files are the truth. The database carries only log_path (relative to
//     the root), log_bytes, log_truncated and the error_tail text.
//   - One file per attempt, at <date>/<run_id>/<step>.<attempt>.ndjson under
//     the log root. The date shard makes retention a directory removal.
//   - Every line carries a seq number, including lines the quota throws
//     away, so loss is detectable from the file alone.
//   - The quota is 16 MiB per attempt: the first quarter stays on disk as
//     the head, the last three quarters are kept in memory and written
//     behind a truncated marker at Finish.
//   - The job process sees pipes. Nothing here hands out a file handle to
//     the process whose output it is collecting.
//   - Directories are 0700 and files are 0600, checked after creation and
//     again on every path that already existed. Anything wider is refused.
//
// Time comes from a clock.Clock, never from the package time constructors,
// so a test can freeze the timestamps and the date shard.
