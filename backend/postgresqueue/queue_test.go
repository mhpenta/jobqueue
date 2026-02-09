//go:build integration
// +build integration

package postgresqueue_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/mhpenta/jobqueue"
	"github.com/mhpenta/jobqueue/backend/postgresqueue"
)

type TestPayload struct {
	Message   string `json:"message"`
	ShouldErr bool   `json:"should_err"`
}

// testSetup is an integration-only helper; it requires JOBQUEUE_POSTGRES_DSN.
func testSetup(t *testing.T) (*sql.DB, jobqueue.Queue) {
	t.Helper()

	return testSetupWithConfig(t, jobqueue.Config{
		MaxRetries:        3,
		VisibilityTimeout: 1 * time.Second,
	})
}

func testSetupWithConfig(t *testing.T, config jobqueue.Config) (*sql.DB, jobqueue.Queue) {
	t.Helper()

	dsn := os.Getenv("JOBQUEUE_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("JOBQUEUE_POSTGRES_DSN not set")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	if err := applySchema(db); err != nil {
		db.Close()
		t.Fatalf("failed to apply schema: %v", err)
	}

	if err := truncateTables(db); err != nil {
		db.Close()
		t.Fatalf("failed to truncate tables: %v", err)
	}

	queue := postgresqueue.New(db, config)

	t.Cleanup(func() {
		db.Close()
	})

	return db, queue
}

func applySchema(db *sql.DB) error {
	if err := dropTables(db); err != nil {
		return err
	}
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return errors.New("failed to locate test file path")
	}
	schemaPath := filepath.Join(filepath.Dir(filename), "schema", "schema", "001_queue.sql")
	schema, err := os.ReadFile(schemaPath)
	if err != nil {
		return err
	}
	_, err = db.Exec(string(schema))
	return err
}

func dropTables(db *sql.DB) error {
	_, err := db.Exec(`DROP TABLE IF EXISTS job_queue_dlq; DROP TABLE IF EXISTS job_queue;`)
	return err
}

func truncateTables(db *sql.DB) error {
	_, err := db.Exec(`TRUNCATE TABLE job_queue, job_queue_dlq;`)
	return err
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}
	return b
}

func TestEnqueueDequeue(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	body := mustMarshal(t, TestPayload{Message: "hello"})
	id, err := queue.Enqueue(ctx, "test-queue", "test-job", body)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	if id == "" {
		t.Fatal("Enqueue returned empty ID")
	}

	msg, err := queue.Dequeue(ctx, "test-queue")
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if msg == nil {
		t.Fatal("Dequeue returned nil")
	}
	if msg.ID != id {
		t.Errorf("msg.ID = %q, want %q", msg.ID, id)
	}
	if msg.JobType != "test-job" {
		t.Errorf("msg.JobType = %q, want %q", msg.JobType, "test-job")
	}

	var payload TestPayload
	if err := json.Unmarshal(msg.Body, &payload); err != nil {
		t.Fatalf("failed to unmarshal body: %v", err)
	}
	if payload.Message != "hello" {
		t.Errorf("payload.Message = %q, want %q", payload.Message, "hello")
	}
}

func TestDequeueEmptyQueue(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	msg, err := queue.Dequeue(ctx, "test-queue")
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if msg != nil {
		t.Errorf("Dequeue returned msg %v, want nil", msg)
	}
}

func TestRetryAndMaxRetries(t *testing.T) {
	db, queue := testSetup(t)
	ctx := context.Background()

	body := mustMarshal(t, TestPayload{Message: "test"})
	queue.Enqueue(ctx, "test-queue", "test-job", body)

	for i := 0; i < 3; i++ {
		msg, _ := queue.Dequeue(ctx, "test-queue")
		if msg == nil {
			t.Fatalf("attempt %d: got nil", i+1)
		}
		if err := queue.Retry(ctx, msg.ID); err != nil {
			t.Fatalf("Retry failed: %v", err)
		}
	}

	msg, _ := queue.Dequeue(ctx, "test-queue")
	if msg != nil {
		t.Errorf("job should not be dequeued after max retries, retry_count = %d", msg.RetryCount)
	}

	var retryCount int
	err := db.QueryRow("SELECT retry_count FROM job_queue WHERE queue_name = 'test-queue'").Scan(&retryCount)
	if err != nil {
		t.Fatalf("failed to query retry_count: %v", err)
	}
	if retryCount != 3 {
		t.Errorf("retry_count = %d, want 3", retryCount)
	}
}

func TestMaxRetriesZeroAllowsFirstAttempt(t *testing.T) {
	_, queue := testSetupWithConfig(t, jobqueue.Config{
		MaxRetries:        0,
		VisibilityTimeout: 1 * time.Second,
	})
	ctx := context.Background()

	queue.Enqueue(ctx, "test-queue", "test-job", mustMarshal(t, TestPayload{Message: "zero"}))

	msg, err := queue.Dequeue(ctx, "test-queue")
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if msg == nil {
		t.Fatal("expected first attempt to be dequeued when MaxRetries = 0")
	}
	if msg.ShouldRetry() {
		t.Error("ShouldRetry should be false when MaxRetries = 0")
	}
}

func TestFailMovesToDLQ(t *testing.T) {
	db, queue := testSetup(t)
	ctx := context.Background()

	body := mustMarshal(t, TestPayload{Message: "test"})
	id, _ := queue.Enqueue(ctx, "test-queue", "test-job", body)
	queue.Dequeue(ctx, "test-queue")

	testErr := errors.New("something went wrong")
	if err := queue.Fail(ctx, id, testErr); err != nil {
		t.Fatalf("Fail failed: %v", err)
	}

	var dlqError string
	err := db.QueryRow("SELECT error FROM job_queue_dlq WHERE id = $1", id).Scan(&dlqError)
	if err != nil {
		t.Fatalf("failed to query DLQ: %v", err)
	}
	if dlqError != "something went wrong" {
		t.Errorf("DLQ error = %q, want %q", dlqError, "something went wrong")
	}
}

func TestFailOnCompletedJob(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	body := mustMarshal(t, TestPayload{Message: "done"})
	id, _ := queue.Enqueue(ctx, "test-queue", "test-job", body)
	queue.Dequeue(ctx, "test-queue")
	queue.Complete(ctx, id)

	err := queue.Fail(ctx, id, errors.New("late fail"))
	if !errors.Is(err, jobqueue.ErrJobCompleted) {
		t.Fatalf("expected ErrJobCompleted, got %v", err)
	}
}

func TestRetryOnCompletedJob(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	body := mustMarshal(t, TestPayload{Message: "done"})
	id, _ := queue.Enqueue(ctx, "test-queue", "test-job", body)
	queue.Dequeue(ctx, "test-queue")
	queue.Complete(ctx, id)

	err := queue.Retry(ctx, id)
	if !errors.Is(err, jobqueue.ErrJobNotPending) {
		t.Fatalf("expected ErrJobNotPending, got %v", err)
	}
}

func TestComplete(t *testing.T) {
	db, queue := testSetup(t)
	ctx := context.Background()

	body := mustMarshal(t, TestPayload{Message: "test"})
	id, _ := queue.Enqueue(ctx, "test-queue", "test-job", body)
	msg, _ := queue.Dequeue(ctx, "test-queue")

	if err := queue.Complete(ctx, msg.ID); err != nil {
		t.Fatalf("Complete failed: %v", err)
	}

	var completedAt sql.NullInt64
	err := db.QueryRow("SELECT completed_at FROM job_queue WHERE id = $1", id).Scan(&completedAt)
	if err != nil {
		t.Fatalf("failed to query job: %v", err)
	}
	if !completedAt.Valid {
		t.Error("completed_at should be set")
	}
}

func TestCompleteOnNonexistentJob(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	err := queue.Complete(ctx, "nonexistent-id")
	if !errors.Is(err, jobqueue.ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}
