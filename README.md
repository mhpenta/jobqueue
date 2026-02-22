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
