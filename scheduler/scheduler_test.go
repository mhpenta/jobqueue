package scheduler

import (
	"context"
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/mhpenta/jobqueue"
)

type fakeQueue struct {
	lastQueueName string
	lastJobType   string
	lastBody      []byte
	lastOptions   jobqueue.EnqueueOptions
}

func (f *fakeQueue) Enqueue(ctx context.Context, queueName string, jobType string, body []byte, opts ...jobqueue.EnqueueOption) (string, error) {
	_ = ctx
	f.lastQueueName = queueName
	f.lastJobType = jobType
	f.lastBody = body
	f.lastOptions = jobqueue.ResolveEnqueueOptions(opts)
	return "job-123", nil
}

func (f *fakeQueue) Dequeue(context.Context, string) (*jobqueue.Message, error) { return nil, nil }
func (f *fakeQueue) Complete(context.Context, string) error                      { return nil }
func (f *fakeQueue) Retry(context.Context, string) error                         { return nil }
func (f *fakeQueue) Fail(context.Context, string, error) error                   { return nil }

func TestNextRunAtRespectsMinGapPerKey(t *testing.T) {
	loc := time.UTC
	baseNow := time.Date(2026, 2, 22, 9, 0, 0, 0, loc)

	store := NewMemoryStore()
	queue := &fakeQueue{}
	s, err := New(
		queue,
		store,
		WithClock(func() time.Time { return baseNow }),
		WithRandSource(rand.NewSource(1)),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	p := Policy{MinGap: 30 * time.Minute}
	first, err := s.NextRunAt(context.Background(), "acct-1", time.Time{}, p)
	if err != nil {
		t.Fatalf("first NextRunAt() error: %v", err)
	}
	second, err := s.NextRunAt(context.Background(), "acct-1", time.Time{}, p)
	if err != nil {
		t.Fatalf("second NextRunAt() error: %v", err)
	}

	if !first.Equal(baseNow) {
		t.Fatalf("first = %v, want %v", first, baseNow)
	}
	wantSecond := baseNow.Add(30 * time.Minute)
	if !second.Equal(wantSecond) {
		t.Fatalf("second = %v, want %v", second, wantSecond)
	}
}

func TestNextRunAtIsIndependentAcrossKeys(t *testing.T) {
	now := time.Date(2026, 2, 22, 9, 0, 0, 0, time.UTC)
	s, err := New(
		&fakeQueue{},
		NewMemoryStore(),
		WithClock(func() time.Time { return now }),
		WithRandSource(rand.NewSource(2)),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	p := Policy{MinGap: time.Hour}
	a1, _ := s.NextRunAt(context.Background(), "a", time.Time{}, p)
	a2, _ := s.NextRunAt(context.Background(), "a", time.Time{}, p)
	b1, _ := s.NextRunAt(context.Background(), "b", time.Time{}, p)

	if !a1.Equal(now) {
		t.Fatalf("a1 = %v, want %v", a1, now)
	}
	if !a2.Equal(now.Add(time.Hour)) {
		t.Fatalf("a2 = %v, want %v", a2, now.Add(time.Hour))
	}
	if !b1.Equal(now) {
		t.Fatalf("b1 = %v, want %v", b1, now)
	}
}

func TestNextRunAtAlignsToWindow(t *testing.T) {
	loc := time.FixedZone("EST", -5*60*60)
	now := time.Date(2026, 2, 22, 8, 15, 0, 0, loc)

	s, err := New(
		&fakeQueue{},
		NewMemoryStore(),
		WithClock(func() time.Time { return now }),
		WithRandSource(rand.NewSource(3)),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	p := Policy{
		Window: &DailyWindow{
			Start:    10 * time.Hour,
			End:      16 * time.Hour,
			Location: loc,
		},
	}

	got, err := s.NextRunAt(context.Background(), "k", time.Time{}, p)
	if err != nil {
		t.Fatalf("NextRunAt() error: %v", err)
	}
	want := time.Date(2026, 2, 22, 10, 0, 0, 0, loc)
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestNextRunAtWindowRollsToNextDay(t *testing.T) {
	loc := time.FixedZone("EST", -5*60*60)
	now := time.Date(2026, 2, 22, 17, 0, 0, 0, loc)

	s, err := New(
		&fakeQueue{},
		NewMemoryStore(),
		WithClock(func() time.Time { return now }),
		WithRandSource(rand.NewSource(4)),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	p := Policy{
		Window: &DailyWindow{
			Start:    10 * time.Hour,
			End:      16 * time.Hour,
			Location: loc,
		},
	}

	got, err := s.NextRunAt(context.Background(), "k", time.Time{}, p)
	if err != nil {
		t.Fatalf("NextRunAt() error: %v", err)
	}
	want := time.Date(2026, 2, 23, 10, 0, 0, 0, loc)
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestNextRunAtJitterStaysInsideWindow(t *testing.T) {
	loc := time.UTC
	now := time.Date(2026, 2, 22, 9, 0, 0, 0, loc)

	s, err := New(
		&fakeQueue{},
		NewMemoryStore(),
		WithClock(func() time.Time { return now }),
		WithRandSource(rand.NewSource(5)),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	p := Policy{
		Window: &DailyWindow{
			Start:    10 * time.Hour,
			End:      10*time.Hour + 5*time.Minute,
			Location: loc,
		},
		MaxJitter: 10 * time.Minute,
	}

	got, err := s.NextRunAt(context.Background(), "k", time.Time{}, p)
	if err != nil {
		t.Fatalf("NextRunAt() error: %v", err)
	}

	windowStart := time.Date(2026, 2, 22, 10, 0, 0, 0, loc)
	windowEnd := time.Date(2026, 2, 22, 10, 5, 0, 0, loc)
	if got.Before(windowStart) || got.After(windowEnd) {
		t.Fatalf("jittered time %v outside [%v, %v]", got, windowStart, windowEnd)
	}
}

func TestEnqueueUsesComputedRunAt(t *testing.T) {
	loc := time.UTC
	now := time.Date(2026, 2, 22, 9, 0, 0, 0, loc)

	fq := &fakeQueue{}
	s, err := New(
		fq,
		NewMemoryStore(),
		WithClock(func() time.Time { return now }),
		WithRandSource(rand.NewSource(6)),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	body, _ := json.Marshal(map[string]string{"x": "1"})
	id, runAt, err := s.Enqueue(context.Background(), EnqueueRequest{
		QueueName: "work",
		JobType:   "sync",
		JobKey:    "acct-42",
		Body:      body,
		Policy: Policy{
			Window: &DailyWindow{
				Start:    10 * time.Hour,
				End:      16 * time.Hour,
				Location: loc,
			},
		},
		Options: []jobqueue.EnqueueOption{jobqueue.WithPriority(9)},
	})
	if err != nil {
		t.Fatalf("Enqueue() error: %v", err)
	}
	if id != "job-123" {
		t.Fatalf("id = %q, want %q", id, "job-123")
	}

	wantRunAt := time.Date(2026, 2, 22, 10, 0, 0, 0, loc)
	if !runAt.Equal(wantRunAt) {
		t.Fatalf("runAt = %v, want %v", runAt, wantRunAt)
	}

	if fq.lastQueueName != "work" {
		t.Fatalf("queueName = %q, want %q", fq.lastQueueName, "work")
	}
	if fq.lastJobType != "sync" {
		t.Fatalf("jobType = %q, want %q", fq.lastJobType, "sync")
	}
	if fq.lastOptions.Priority != 9 {
		t.Fatalf("priority = %d, want %d", fq.lastOptions.Priority, 9)
	}
	if !fq.lastOptions.RunAt.Equal(wantRunAt) {
		t.Fatalf("run option RunAt = %v, want %v", fq.lastOptions.RunAt, wantRunAt)
	}
}
