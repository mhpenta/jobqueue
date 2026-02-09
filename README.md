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
