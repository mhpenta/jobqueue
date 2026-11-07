# jobqueue

Minimal, persisted job queue for Postgres or Turso (SQLite) in Go.

## Install

```bash
go get github.com/mhpenta/jobqueue
```

## Usage

```go
queue := tursoqueue.New(db, jobqueue.DefaultConfig())

body, _ := json.Marshal(map[string]string{"to": "alice@example.com"})
id, _ := queue.Enqueue(ctx, "emails", "send-email", body)

msg, _ := queue.Dequeue(ctx, "emails")
if msg != nil {
	// process msg.Body
	queue.Complete(ctx, id)
}
```

## Retries, Leases, and the Dead-Letter Queue

`Dequeue` claims a job for `Config.VisibilityTimeout`. After a failure:

```go
if msg.ShouldRetry() {
	queue.Retry(ctx, msg.ID) // another attempt, after Config.RetryBackoff if set
} else {
	queue.Fail(ctx, msg.ID, err) // dead-letter it, keeping the real error
}
```

- `MaxRetries` is the total number of attempts (`0` still allows one).
- A claim whose visibility timeout runs out without `Complete`, `Retry`, or
  `Fail` counts as a failed attempt, so a job that crashes its worker is not
  redelivered forever.
- A `Retry` that would use up the last attempt moves the job to the
  dead-letter queue. A job whose lease expires on its last attempt is moved
  there by `Admin.SweepStuckJobs`.
- Completing a job twice is a no-op; the first completion's time and result
  are kept.

`Complete`, `Retry`, and `Fail` take only the job ID and do not check which
worker holds the claim, so keep `VisibilityTimeout` longer than your slowest
job.

## Backend Schema Bootstrap

The Postgres backend exports its canonical schema so callers do not need to copy
the queue DDL into their own application bootstrap:

```go
if err := postgresqueue.EnsureSchema(ctx, db); err != nil {
	log.Fatal(err)
}

queue := postgresqueue.New(db, jobqueue.DefaultConfig())
```

If you only need the SQL string, use `postgresqueue.SchemaSQL()`.

## Policy-Based Scheduling Helper

The `scheduler` helper package supports:
- per-key spacing (`MinGap`)
- daily run windows (for example, only 10:00 to 16:00 in a timezone)
- optional jitter

```go
store := scheduler.NewMemoryStore()
sched, _ := scheduler.New(queue, store)

id, runAt, err := sched.Enqueue(ctx, scheduler.EnqueueRequest{
	QueueName: "my-queue",
	JobType:   "my-job",
	JobKey:    "customer-123",
	Body:      body,
	Policy: scheduler.Policy{
		MinGap:    30 * time.Minute,
		MaxJitter: 10 * time.Minute,
		Window: &scheduler.DailyWindow{
			Start:    10 * time.Hour,
			End:      16 * time.Hour,
			Location: time.FixedZone("EST", -5*60*60),
		},
	},
	Options: []jobqueue.EnqueueOption{
		jobqueue.WithPriority(5),
	},
})
_ = id
_ = runAt
_ = err
```

`scheduler.NewMemoryStore()` is in-process only. For distributed producers, implement `scheduler.KeyStateStore` with an atomic, shared backend (for example, SQL transaction + per-key row lock).

## Readable SQL Views

Both backend schema files now include additive read-only views:
- `job_queue_readable`
- `job_queue_dlq_readable`

They keep Unix timestamp storage unchanged but expose:
- derived human timestamps
- derived status (`pending`, `scheduled`, `completed`, `exhausted`)
- body size + short preview column

Example:

```sql
SELECT id, queue_name, job_type, derived_status, created_at_ts, visible_after_ts
FROM job_queue_readable
ORDER BY created_at DESC
LIMIT 50;
```
