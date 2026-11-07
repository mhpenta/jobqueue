//go:build integration
// +build integration

package postgresqueue_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/mhpenta/jobqueue"
)

// expireLease backdates a claimed job's lease so it reads as expired.
func expireLease(t *testing.T, db *sql.DB, id string) {
	t.Helper()
	if _, err := db.Exec("UPDATE job_queue SET visible_after = $1 WHERE id = $2", time.Now().Add(-time.Minute).Unix(), id); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
}

// simulateLegacyRetry writes the row state a pre-v0.5.0 Retry left behind.
func simulateLegacyRetry(t *testing.T, db *sql.DB, id string) {
	t.Helper()
	if _, err := db.Exec("UPDATE job_queue SET visible_after = 0, retry_count = retry_count + 1 WHERE id = $1", id); err != nil {
		t.Fatalf("simulate legacy retry: %v", err)
	}
}

func TestExpiredLeaseCountsAsAttempt(t *testing.T) {
	db, queue := testSetup(t)
	ctx := context.Background()

	id, _ := queue.Enqueue(ctx, "test-queue", "test-job", mustMarshal(t, TestPayload{Message: "crash"}))
	if msg, _ := queue.Dequeue(ctx, "test-queue"); msg == nil || msg.RetryCount != 0 {
		t.Fatalf("first dequeue = %+v, want RetryCount 0", msg)
	}

	// The worker dies without Complete, Retry, or Fail.
	expireLease(t, db, id)

	msg, err := queue.Dequeue(ctx, "test-queue")
	if err != nil || msg == nil {
		t.Fatalf("redelivery: msg=%v err=%v", msg, err)
	}
	if msg.RetryCount != 1 {
		t.Errorf("RetryCount after expired lease = %d, want 1", msg.RetryCount)
	}
}

func TestExpiredLeaseOnLastAttemptIsSwept(t *testing.T) {
	db, q := testSetupWithConfig(t, jobqueue.Config{MaxRetries: 1, VisibilityTimeout: time.Hour})
	admin := q.(jobqueue.Admin)
	ctx := context.Background()

	id, _ := q.Enqueue(ctx, "test-queue", "test-job", mustMarshal(t, TestPayload{Message: "poison"}))
	if msg, _ := q.Dequeue(ctx, "test-queue"); msg == nil {
		t.Fatal("expected first attempt")
	}

	if swept, _ := admin.SweepStuckJobs(ctx, "test-queue"); swept != 0 {
		t.Fatalf("swept a job whose lease is still active: %d", swept)
	}

	expireLease(t, db, id)

	if msg, _ := q.Dequeue(ctx, "test-queue"); msg != nil {
		t.Fatalf("job with no attempts left was redelivered: %+v", msg)
	}
	swept, err := admin.SweepStuckJobs(ctx, "test-queue")
	if err != nil || swept != 1 {
		t.Fatalf("SweepStuckJobs = %d, %v; want 1", swept, err)
	}
	dlq, err := q.(jobqueue.LookupQueue).GetDLQJob(ctx, id)
	if err != nil {
		t.Fatalf("GetDLQJob: %v", err)
	}
	if dlq.RetryCount != 1 || dlq.Error != "swept: visibility timeout expired on the last attempt" {
		t.Errorf("DLQ entry = retry %d, error %q", dlq.RetryCount, dlq.Error)
	}
}

func TestRetryReleasesClaim(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	queue.Enqueue(ctx, "test-queue", "test-job", mustMarshal(t, TestPayload{Message: "retry"}))
	msg, _ := queue.Dequeue(ctx, "test-queue")
	if err := queue.Retry(ctx, msg.ID); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	msg, _ = queue.Dequeue(ctx, "test-queue")
	if msg == nil {
		t.Fatal("expected redelivery after Retry")
	}
	if msg.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1 (Retry must not also count as an expired lease)", msg.RetryCount)
	}
}

func TestLegacyReleasedJobIsNotCountedAsExpired(t *testing.T) {
	db, queue := testSetup(t)
	ctx := context.Background()

	id, _ := queue.Enqueue(ctx, "test-queue", "test-job", mustMarshal(t, TestPayload{Message: "legacy"}))
	queue.Dequeue(ctx, "test-queue")
	// Versions before v0.5.0 released a retried job by setting visible_after
	// to 0 and leaving claimed_at in place.
	simulateLegacyRetry(t, db, id)

	msg, _ := queue.Dequeue(ctx, "test-queue")
	if msg == nil {
		t.Fatal("expected redelivery")
	}
	if msg.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", msg.RetryCount)
	}
}

func TestRetryBackoffDelaysVisibility(t *testing.T) {
	_, queue := testSetupWithConfig(t, jobqueue.Config{
		MaxRetries:        3,
		VisibilityTimeout: time.Second,
		RetryBackoff:      func(int) time.Duration { return time.Hour },
	})
	ctx := context.Background()

	queue.Enqueue(ctx, "test-queue", "test-job", mustMarshal(t, TestPayload{Message: "backoff"}))
	msg, _ := queue.Dequeue(ctx, "test-queue")
	if err := queue.Retry(ctx, msg.ID); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if msg, _ := queue.Dequeue(ctx, "test-queue"); msg != nil {
		t.Error("job became visible before its retry backoff elapsed")
	}

	got, err := queue.(jobqueue.LookupQueue).GetJob(ctx, msg.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", got.RetryCount)
	}
}

func TestCompleteTwiceKeepsFirstResult(t *testing.T) {
	_, queue := testSetup(t)
	ctx := context.Background()

	id, _ := queue.Enqueue(ctx, "test-queue", "test-job", mustMarshal(t, TestPayload{Message: "done"}))
	queue.Dequeue(ctx, "test-queue")

	if err := jobqueue.CompleteWithResult(ctx, queue, id, jobqueue.JobResult{Code: "first"}); err != nil {
		t.Fatalf("first completion: %v", err)
	}
	if err := jobqueue.CompleteWithResult(ctx, queue, id, jobqueue.JobResult{Code: "second"}); err != nil {
		t.Fatalf("second completion should be a no-op, got %v", err)
	}
	got, _ := jobqueue.GetJob(ctx, queue, id)
	if got.TerminalCode != "first" {
		t.Errorf("TerminalCode = %q, want %q", got.TerminalCode, "first")
	}
}

func TestSweepIgnoresUnclaimedJobWithZeroMaxRetries(t *testing.T) {
	_, q := testSetupWithConfig(t, jobqueue.Config{MaxRetries: 0, VisibilityTimeout: time.Second})
	ctx := context.Background()

	q.Enqueue(ctx, "test-queue", "test-job", mustMarshal(t, TestPayload{Message: "waiting"}))

	swept, err := q.(jobqueue.Admin).SweepStuckJobs(ctx, "test-queue")
	if err != nil || swept != 0 {
		t.Fatalf("SweepStuckJobs = %d, %v; want 0 (the job has not run yet)", swept, err)
	}
	if msg, _ := q.Dequeue(ctx, "test-queue"); msg == nil {
		t.Error("job with MaxRetries 0 should still get its one attempt")
	}
}
