package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/mhpenta/jobqueue"
	"github.com/mhpenta/jobqueue/backend/tursoqueue"
	_ "modernc.org/sqlite"
)

// SendEmailRequest is the payload for a send-email job.
type SendEmailRequest struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func main() {
	ctx := context.Background()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := createTables(db); err != nil {
		log.Fatal(err)
	}

	queue := tursoqueue.New(db, jobqueue.DefaultConfig())

	fmt.Println("Enqueueing jobs...")

	body1, _ := json.Marshal(SendEmailRequest{
		To:      "alice@example.com",
		Subject: "Hello Alice",
		Body:    "How are you?",
	})
	id1, _ := queue.Enqueue(ctx, "emails", "send-email", body1)
	fmt.Printf("Enqueued job %s\n", id1)

	body2, _ := json.Marshal(SendEmailRequest{
		To:      "bob@example.com",
		Subject: "Hello Bob",
		Body:    "How are you?",
	})
	id2, _ := queue.Enqueue(ctx, "emails", "send-email", body2, jobqueue.WithPriority(10))
	fmt.Printf("Enqueued job %s (high priority)\n", id2)

	body3, _ := json.Marshal(SendEmailRequest{
		To:      "charlie@example.com",
		Subject: "Hello Charlie",
		Body:    "How are you?",
	})
	id3, _ := queue.Enqueue(ctx, "emails", "send-email", body3, jobqueue.WithDelay(5*time.Second))
	fmt.Printf("Enqueued job %s (delayed 5s)\n", id3)

	fmt.Println("\nProcessing jobs...")

	for {
		msg, err := queue.Dequeue(ctx, "emails")
		if err != nil {
			log.Printf("Dequeue error: %v", err)
			break
		}
		if msg == nil {
			fmt.Println("No more jobs available")
			break
		}

		fmt.Printf("\nProcessing job %s (type: %s, priority: %d, retry: %d)\n",
			msg.ID, msg.JobType, msg.Priority, msg.RetryCount)

		switch msg.JobType {
		case "send-email":
			var req SendEmailRequest
			if err := json.Unmarshal(msg.Body, &req); err != nil {
				queue.Fail(ctx, msg.ID, err)
				continue
			}
			fmt.Printf("Sending email to %s: %s\n", req.To, req.Subject)
			queue.Complete(ctx, msg.ID)

		default:
			queue.Fail(ctx, msg.ID, fmt.Errorf("unknown job type: %s", msg.JobType))
		}
	}

	fmt.Println("\nDone!")
}

func createTables(db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS job_queue (
		id            TEXT PRIMARY KEY NOT NULL,
		queue_name    TEXT NOT NULL,
		job_type      TEXT NOT NULL,
		body          BLOB NOT NULL,
		priority      INTEGER NOT NULL DEFAULT 0,
		visible_after INTEGER NOT NULL,
		created_at    INTEGER NOT NULL,
		retry_count   INTEGER NOT NULL DEFAULT 0,
		max_retries   INTEGER NOT NULL DEFAULT 3,
		completed_at  INTEGER
	);

	CREATE INDEX IF NOT EXISTS idx_job_queue_dequeue
	ON job_queue(queue_name, completed_at, visible_after, priority DESC, created_at ASC);

	CREATE TABLE IF NOT EXISTS job_queue_dlq (
		id          TEXT PRIMARY KEY NOT NULL,
		queue_name  TEXT NOT NULL,
		job_type    TEXT NOT NULL,
		body        BLOB NOT NULL,
		priority    INTEGER NOT NULL,
		created_at  INTEGER NOT NULL,
		failed_at   INTEGER NOT NULL,
		retry_count INTEGER NOT NULL,
		error       TEXT NOT NULL
	);

	CREATE INDEX IF NOT EXISTS idx_job_queue_dlq_queue
	ON job_queue_dlq(queue_name, failed_at DESC);
	`

	_, err := db.Exec(schema)
	return err
}
