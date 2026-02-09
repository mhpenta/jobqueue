package tursoqueue_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/mhpenta/jobqueue"
	"github.com/mhpenta/jobqueue/backend/tursoqueue"
	_ "modernc.org/sqlite"
)

type TestPayload struct {
	Message   string `json:"message"`
	ShouldErr bool   `json:"should_err"`
}

func testSetup(t *testing.T) (*sql.DB, jobqueue.Queue) {
	t.Helper()

	return testSetupWithConfig(t, jobqueue.Config{
		MaxRetries:        3,
		VisibilityTimeout: 1 * time.Second,
	})
}

func testSetupWithConfig(t *testing.T, config jobqueue.Config) (*sql.DB, jobqueue.Queue) {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	schema := `
	CREATE TABLE job_queue (
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

	CREATE INDEX idx_job_queue_dequeue
	ON job_queue(queue_name, completed_at, visible_after, priority DESC, created_at ASC);

	CREATE TABLE job_queue_dlq (
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
	`

	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	queue := tursoqueue.New(db, config)

	t.Cleanup(func() {
		db.Close()
	})

	return db, queue
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
	err := db.QueryRow("SELECT completed_at FROM job_queue WHERE id = ?", id).Scan(&completedAt)
	if err != nil {
		t.Fatalf("failed to query job: %v", err)
	}
	if !completedAt.Valid {
		t.Error("completed_at should be set")
	}

	msg2, _ := queue.Dequeue(ctx, "test-queue")
	if msg2 != nil {
		t.Error("completed job should not be dequeued")
	}
}

func TestRetry(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	body := mustMarshal(t, TestPayload{Message: "test"})
	id, _ := queue.Enqueue(ctx, "test-queue", "test-job", body)
	msg, _ := queue.Dequeue(ctx, "test-queue")

	if err := queue.Retry(ctx, msg.ID); err != nil {
		t.Fatalf("Retry failed: %v", err)
	}

	msg2, err := queue.Dequeue(ctx, "test-queue")
	if err != nil {
		t.Fatalf("Dequeue after retry failed: %v", err)
	}
	if msg2 == nil {
		t.Fatal("retried job should be dequeued")
	}
	if msg2.ID != id {
		t.Errorf("msg.ID = %q, want %q", msg2.ID, id)
	}
	if msg2.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", msg2.RetryCount)
	}
}

func TestFail(t *testing.T) {
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
	err := db.QueryRow("SELECT error FROM job_queue_dlq WHERE id = ?", id).Scan(&dlqError)
	if err != nil {
		t.Fatalf("failed to query DLQ: %v", err)
	}
	if dlqError != "something went wrong" {
		t.Errorf("DLQ error = %q, want %q", dlqError, "something went wrong")
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM job_queue WHERE id = ?", id).Scan(&count)
	if count != 0 {
		t.Error("job should be removed from main queue")
	}
}

func TestPriorityOrdering(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	queue.Enqueue(ctx, "test-queue", "test-job", mustMarshal(t, TestPayload{Message: "low"}), jobqueue.WithPriority(1))
	queue.Enqueue(ctx, "test-queue", "test-job", mustMarshal(t, TestPayload{Message: "high"}), jobqueue.WithPriority(10))
	queue.Enqueue(ctx, "test-queue", "test-job", mustMarshal(t, TestPayload{Message: "medium"}), jobqueue.WithPriority(5))

	expected := []string{"high", "medium", "low"}
	for i, exp := range expected {
		msg, _ := queue.Dequeue(ctx, "test-queue")
		if msg == nil {
			t.Fatalf("job %d: got nil", i)
		}
		var payload TestPayload
		json.Unmarshal(msg.Body, &payload)
		if payload.Message != exp {
			t.Errorf("job %d: message = %q, want %q", i, payload.Message, exp)
		}
		queue.Complete(ctx, msg.ID)
	}
}

func TestDelayedJob(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	body := mustMarshal(t, TestPayload{Message: "delayed"})
	queue.Enqueue(ctx, "test-queue", "test-job", body, jobqueue.WithDelay(2*time.Second))

	msg, _ := queue.Dequeue(ctx, "test-queue")
	if msg != nil {
		t.Error("delayed job should not be visible yet")
	}

	time.Sleep(2100 * time.Millisecond)

	msg, _ = queue.Dequeue(ctx, "test-queue")
	if msg == nil {
		t.Fatal("delayed job should be visible after delay")
	}
	var payload TestPayload
	json.Unmarshal(msg.Body, &payload)
	if payload.Message != "delayed" {
		t.Errorf("message = %q, want %q", payload.Message, "delayed")
	}
}

func TestVisibilityTimeout(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	body := mustMarshal(t, TestPayload{Message: "test"})
	queue.Enqueue(ctx, "test-queue", "test-job", body)

	msg1, _ := queue.Dequeue(ctx, "test-queue")
	if msg1 == nil {
		t.Fatal("first dequeue should return job")
	}

	msg2, _ := queue.Dequeue(ctx, "test-queue")
	if msg2 != nil {
		t.Error("job should be invisible")
	}

	time.Sleep(1100 * time.Millisecond)

	msg3, _ := queue.Dequeue(ctx, "test-queue")
	if msg3 == nil {
		t.Fatal("job should be visible after timeout")
	}
	if msg3.ID != msg1.ID {
		t.Errorf("msg.ID = %q, want %q", msg3.ID, msg1.ID)
	}
}

func TestMaxRetries(t *testing.T) {
	db, queue := testSetup(t)
	ctx := context.Background()

	body := mustMarshal(t, TestPayload{Message: "test"})
	queue.Enqueue(ctx, "test-queue", "test-job", body)

	for i := 0; i < 3; i++ {
		msg, _ := queue.Dequeue(ctx, "test-queue")
		if msg == nil {
			t.Fatalf("attempt %d: got nil", i+1)
		}
		queue.Retry(ctx, msg.ID)
	}

	msg, _ := queue.Dequeue(ctx, "test-queue")
	if msg != nil {
		t.Errorf("job should not be dequeued after max retries, retry_count = %d", msg.RetryCount)
	}

	var retryCount int
	db.QueryRow("SELECT retry_count FROM job_queue WHERE queue_name = 'test-queue'").Scan(&retryCount)
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

func TestMultipleQueues(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	queue.Enqueue(ctx, "queue-a", "test-job", mustMarshal(t, TestPayload{Message: "A"}))
	queue.Enqueue(ctx, "queue-b", "test-job", mustMarshal(t, TestPayload{Message: "B"}))

	msgA, _ := queue.Dequeue(ctx, "queue-a")
	if msgA == nil {
		t.Fatal("queue-a should have job")
	}
	var payloadA TestPayload
	json.Unmarshal(msgA.Body, &payloadA)
	if payloadA.Message != "A" {
		t.Error("wrong job from queue-a")
	}

	msgB, _ := queue.Dequeue(ctx, "queue-b")
	if msgB == nil {
		t.Fatal("queue-b should have job")
	}
	var payloadB TestPayload
	json.Unmarshal(msgB.Body, &payloadB)
	if payloadB.Message != "B" {
		t.Error("wrong job from queue-b")
	}

	msgA2, _ := queue.Dequeue(ctx, "queue-a")
	if msgA2 != nil {
		t.Error("queue-a should be empty")
	}
}

func TestBodyRoundTrip(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	type ComplexPayload struct {
		AccessionNumber string   `json:"accession_number"`
		CIK             string   `json:"cik"`
		Tags            []string `json:"tags"`
		Priority        float64  `json:"priority"`
	}

	original := ComplexPayload{
		AccessionNumber: "0001213900-26-010261",
		CIK:             "1969401",
		Tags:            []string{"10-K", "annual", "xbrl"},
		Priority:        99.5,
	}

	body := mustMarshal(t, original)
	queue.Enqueue(ctx, "test-queue", "xbrl-process", body)

	msg, _ := queue.Dequeue(ctx, "test-queue")
	if msg == nil {
		t.Fatal("expected message")
	}

	var roundTripped ComplexPayload
	if err := json.Unmarshal(msg.Body, &roundTripped); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if roundTripped.AccessionNumber != original.AccessionNumber {
		t.Errorf("AccessionNumber = %q, want %q", roundTripped.AccessionNumber, original.AccessionNumber)
	}
	if roundTripped.CIK != original.CIK {
		t.Errorf("CIK = %q, want %q", roundTripped.CIK, original.CIK)
	}
	if len(roundTripped.Tags) != 3 {
		t.Errorf("Tags len = %d, want 3", len(roundTripped.Tags))
	}
	if roundTripped.Priority != 99.5 {
		t.Errorf("Priority = %f, want 99.5", roundTripped.Priority)
	}
}

func TestMultipleJobTypesOnSameQueue(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	queue.Enqueue(ctx, "work", "send-email", mustMarshal(t, map[string]string{"to": "alice@example.com"}))
	queue.Enqueue(ctx, "work", "process-xbrl", mustMarshal(t, map[string]string{"accession": "123"}))
	queue.Enqueue(ctx, "work", "generate-report", mustMarshal(t, map[string]string{"company": "AAPL"}))

	types := make(map[string]bool)
	for i := 0; i < 3; i++ {
		msg, err := queue.Dequeue(ctx, "work")
		if err != nil {
			t.Fatalf("Dequeue %d failed: %v", i, err)
		}
		if msg == nil {
			t.Fatalf("Dequeue %d returned nil", i)
		}
		types[msg.JobType] = true
		queue.Complete(ctx, msg.ID)
	}

	for _, jt := range []string{"send-email", "process-xbrl", "generate-report"} {
		if !types[jt] {
			t.Errorf("missing job type %q", jt)
		}
	}
}

func TestEnqueueEmptyBody(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	id, err := queue.Enqueue(ctx, "test-queue", "ping", []byte("{}"))
	if err != nil {
		t.Fatalf("Enqueue empty body failed: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty ID")
	}

	msg, _ := queue.Dequeue(ctx, "test-queue")
	if msg == nil {
		t.Fatal("expected message")
	}
	if string(msg.Body) != "{}" {
		t.Errorf("body = %q, want %q", string(msg.Body), "{}")
	}
}

func TestFailPreservesBodyInDLQ(t *testing.T) {
	db, queue := testSetup(t)
	ctx := context.Background()

	originalBody := mustMarshal(t, TestPayload{Message: "important-data"})
	id, _ := queue.Enqueue(ctx, "test-queue", "critical-job", originalBody)
	queue.Dequeue(ctx, "test-queue")

	queue.Fail(ctx, id, errors.New("kaboom"))

	var dlqBody []byte
	var dlqJobType string
	err := db.QueryRow("SELECT body, job_type FROM job_queue_dlq WHERE id = ?", id).Scan(&dlqBody, &dlqJobType)
	if err != nil {
		t.Fatalf("failed to query DLQ: %v", err)
	}

	if dlqJobType != "critical-job" {
		t.Errorf("DLQ job_type = %q, want %q", dlqJobType, "critical-job")
	}

	var payload TestPayload
	if err := json.Unmarshal(dlqBody, &payload); err != nil {
		t.Fatalf("failed to unmarshal DLQ body: %v", err)
	}
	if payload.Message != "important-data" {
		t.Errorf("DLQ payload.Message = %q, want %q", payload.Message, "important-data")
	}
}

func TestFailOnNonexistentJob(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	err := queue.Fail(ctx, "nonexistent-id", errors.New("oops"))
	if err == nil {
		t.Error("Fail on nonexistent job should return error")
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

func TestCompleteOnNonexistentJob(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	err := queue.Complete(ctx, "nonexistent-id")
	if !errors.Is(err, jobqueue.ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestShouldRetry(t *testing.T) {
	msg := &jobqueue.Message{
		RetryCount: 2,
		MaxRetries: 3,
	}
	if !msg.ShouldRetry() {
		t.Error("ShouldRetry should be true when RetryCount < MaxRetries")
	}

	msg.RetryCount = 3
	if msg.ShouldRetry() {
		t.Error("ShouldRetry should be false when RetryCount >= MaxRetries")
	}

	msg.RetryCount = 5
	if msg.ShouldRetry() {
		t.Error("ShouldRetry should be false when RetryCount > MaxRetries")
	}
}

func TestDequeueMetadata(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	body := mustMarshal(t, TestPayload{Message: "meta"})
	queue.Enqueue(ctx, "my-queue", "my-type", body, jobqueue.WithPriority(7))

	msg, _ := queue.Dequeue(ctx, "my-queue")
	if msg == nil {
		t.Fatal("expected message")
	}

	if msg.QueueName != "my-queue" {
		t.Errorf("QueueName = %q, want %q", msg.QueueName, "my-queue")
	}
	if msg.JobType != "my-type" {
		t.Errorf("JobType = %q, want %q", msg.JobType, "my-type")
	}
	if msg.Priority != 7 {
		t.Errorf("Priority = %d, want 7", msg.Priority)
	}
	if msg.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", msg.MaxRetries)
	}
	if msg.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0", msg.RetryCount)
	}
	if msg.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}
}

func TestFIFOWithinSamePriority(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	queue.Enqueue(ctx, "test-queue", "test-job", mustMarshal(t, TestPayload{Message: "first"}))
	queue.Enqueue(ctx, "test-queue", "test-job", mustMarshal(t, TestPayload{Message: "second"}))
	queue.Enqueue(ctx, "test-queue", "test-job", mustMarshal(t, TestPayload{Message: "third"}))

	expected := []string{"first", "second", "third"}
	for i, exp := range expected {
		msg, _ := queue.Dequeue(ctx, "test-queue")
		if msg == nil {
			t.Fatalf("job %d: got nil", i)
		}
		var payload TestPayload
		json.Unmarshal(msg.Body, &payload)
		if payload.Message != exp {
			t.Errorf("job %d: message = %q, want %q", i, payload.Message, exp)
		}
		queue.Complete(ctx, msg.ID)
	}
}

func TestCompleteIsIdempotent(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	body := mustMarshal(t, TestPayload{Message: "test"})
	id, _ := queue.Enqueue(ctx, "test-queue", "test-job", body)
	queue.Dequeue(ctx, "test-queue")

	if err := queue.Complete(ctx, id); err != nil {
		t.Fatalf("first Complete failed: %v", err)
	}
	if err := queue.Complete(ctx, id); err != nil {
		t.Fatalf("second Complete should not error: %v", err)
	}
}

func TestRunAtScheduling(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	future := time.Now().Add(2 * time.Second)
	body := mustMarshal(t, TestPayload{Message: "scheduled"})
	queue.Enqueue(ctx, "test-queue", "test-job", body, jobqueue.WithRunAt(future))

	msg, _ := queue.Dequeue(ctx, "test-queue")
	if msg != nil {
		t.Error("scheduled job should not be visible before RunAt")
	}

	time.Sleep(2100 * time.Millisecond)

	msg, _ = queue.Dequeue(ctx, "test-queue")
	if msg == nil {
		t.Fatal("scheduled job should be visible after RunAt")
	}
}

func TestFailWithNilError(t *testing.T) {
	db, queue := testSetup(t)
	ctx := context.Background()

	body := mustMarshal(t, TestPayload{Message: "nil-err"})
	id, _ := queue.Enqueue(ctx, "test-queue", "test-job", body)
	queue.Dequeue(ctx, "test-queue")

	if err := queue.Fail(ctx, id, nil); err != nil {
		t.Fatalf("Fail with nil error should not panic or error: %v", err)
	}

	var dlqError string
	err := db.QueryRow("SELECT error FROM job_queue_dlq WHERE id = ?", id).Scan(&dlqError)
	if err != nil {
		t.Fatalf("failed to query DLQ: %v", err)
	}
	if dlqError != "" {
		t.Errorf("DLQ error = %q, want empty string", dlqError)
	}
}

func TestAdminCountAndList(t *testing.T) {
	_, q := testSetup(t)
	admin := q.(*tursoqueue.TursoQueue)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		body := mustMarshal(t, TestPayload{Message: "pending"})
		q.Enqueue(ctx, "test-queue", "test-job", body)
	}

	count, err := admin.CountPendingJobs(ctx, "test-queue")
	if err != nil {
		t.Fatalf("CountPendingJobs: %v", err)
	}
	if count != 3 {
		t.Errorf("CountPendingJobs = %d, want 3", count)
	}

	pending, err := admin.ListPendingJobs(ctx, "test-queue", 10, 0)
	if err != nil {
		t.Fatalf("ListPendingJobs: %v", err)
	}
	if len(pending) != 3 {
		t.Errorf("ListPendingJobs returned %d, want 3", len(pending))
	}

	msg, _ := q.Dequeue(ctx, "test-queue")
	q.Complete(ctx, msg.ID)

	completedCount, _ := admin.CountCompletedJobs(ctx, "test-queue")
	if completedCount != 1 {
		t.Errorf("CountCompletedJobs = %d, want 1", completedCount)
	}

	completed, _ := admin.ListCompletedJobs(ctx, "test-queue", 10, 0)
	if len(completed) != 1 {
		t.Errorf("ListCompletedJobs returned %d, want 1", len(completed))
	}
}

func TestAdminDLQOperations(t *testing.T) {
	_, q := testSetup(t)
	admin := q.(*tursoqueue.TursoQueue)
	ctx := context.Background()

	body := mustMarshal(t, TestPayload{Message: "will-fail"})
	id, _ := q.Enqueue(ctx, "test-queue", "test-job", body)
	q.Dequeue(ctx, "test-queue")
	q.Fail(ctx, id, errors.New("kaboom"))

	count, _ := admin.CountDLQJobs(ctx, "test-queue")
	if count != 1 {
		t.Errorf("CountDLQJobs = %d, want 1", count)
	}

	dlqJobs, _ := admin.ListDLQJobs(ctx, "test-queue", 10, 0)
	if len(dlqJobs) != 1 {
		t.Fatalf("ListDLQJobs returned %d, want 1", len(dlqJobs))
	}
	if dlqJobs[0].Error != "kaboom" {
		t.Errorf("DLQ error = %q, want %q", dlqJobs[0].Error, "kaboom")
	}
	if dlqJobs[0].JobType != "test-job" {
		t.Errorf("DLQ job_type = %q, want %q", dlqJobs[0].JobType, "test-job")
	}

	if err := admin.DeleteDLQJob(ctx, id); err != nil {
		t.Fatalf("DeleteDLQJob: %v", err)
	}

	count, _ = admin.CountDLQJobs(ctx, "test-queue")
	if count != 0 {
		t.Errorf("CountDLQJobs after delete = %d, want 0", count)
	}
}

func TestAdminCleanupCompletedJobs(t *testing.T) {
	_, q := testSetup(t)
	admin := q.(*tursoqueue.TursoQueue)
	ctx := context.Background()

	body := mustMarshal(t, TestPayload{Message: "cleanup"})
	id, _ := q.Enqueue(ctx, "test-queue", "test-job", body)
	q.Dequeue(ctx, "test-queue")
	q.Complete(ctx, id)

	count, _ := admin.CountCompletedJobs(ctx, "test-queue")
	if count != 1 {
		t.Fatalf("expected 1 completed, got %d", count)
	}

	if err := admin.CleanupCompletedJobs(ctx, time.Now().Add(1*time.Second)); err != nil {
		t.Fatalf("CleanupCompletedJobs: %v", err)
	}

	count, _ = admin.CountCompletedJobs(ctx, "test-queue")
	if count != 0 {
		t.Errorf("CountCompletedJobs after cleanup = %d, want 0", count)
	}
}

func TestAdminSweepStuckJobs(t *testing.T) {
	_, q := testSetup(t)
	admin := q.(*tursoqueue.TursoQueue)
	ctx := context.Background()

	body := mustMarshal(t, TestPayload{Message: "stuck"})
	q.Enqueue(ctx, "test-queue", "test-job", body)

	for i := 0; i < 3; i++ {
		msg, _ := q.Dequeue(ctx, "test-queue")
		if msg == nil {
			t.Fatalf("attempt %d: got nil", i+1)
		}
		q.Retry(ctx, msg.ID)
	}

	msg, _ := q.Dequeue(ctx, "test-queue")
	if msg != nil {
		t.Fatal("job should be invisible after max retries")
	}

	pending, _ := admin.CountPendingJobs(ctx, "test-queue")
	if pending != 1 {
		t.Fatalf("expected 1 stuck pending job, got %d", pending)
	}

	swept, err := admin.SweepStuckJobs(ctx, "test-queue")
	if err != nil {
		t.Fatalf("SweepStuckJobs: %v", err)
	}
	if swept != 1 {
		t.Errorf("swept = %d, want 1", swept)
	}

	pending, _ = admin.CountPendingJobs(ctx, "test-queue")
	if pending != 0 {
		t.Errorf("pending after sweep = %d, want 0", pending)
	}

	dlqCount, _ := admin.CountDLQJobs(ctx, "test-queue")
	if dlqCount != 1 {
		t.Errorf("DLQ count after sweep = %d, want 1", dlqCount)
	}

	dlqJobs, _ := admin.ListDLQJobs(ctx, "test-queue", 10, 0)
	if len(dlqJobs) != 1 {
		t.Fatalf("expected 1 DLQ job, got %d", len(dlqJobs))
	}
	if dlqJobs[0].Error != "swept: retry count exhausted" {
		t.Errorf("DLQ error = %q, want %q", dlqJobs[0].Error, "swept: retry count exhausted")
	}
}
