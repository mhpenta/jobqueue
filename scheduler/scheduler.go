package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/mhpenta/jobqueue"
)

var (
	ErrNilQueue        = errors.New("scheduler: queue must not be nil")
	ErrNilStore        = errors.New("scheduler: key state store must not be nil")
	ErrEmptyJobKey     = errors.New("scheduler: job key must not be empty")
	ErrNegativeMinGap  = errors.New("scheduler: min gap must be >= 0")
	ErrNegativeJitter  = errors.New("scheduler: max jitter must be >= 0")
	ErrNilLocation     = errors.New("scheduler: window location must not be nil")
	ErrInvalidWindow   = errors.New("scheduler: window start/end must satisfy 0 <= start < end <= 24h")
	ErrEmptyQueueName  = errors.New("scheduler: queue name must not be empty")
	ErrEmptyJobType    = errors.New("scheduler: job type must not be empty")
	ErrNilBody         = errors.New("scheduler: body must not be nil")
	ErrZeroScheduledAt = errors.New("scheduler: scheduled time must not be zero")
)

// DailyWindow constrains scheduling to a local time range each day.
// Start is inclusive and End is exclusive.
type DailyWindow struct {
	Start    time.Duration
	End      time.Duration
	Location *time.Location
}

func (w DailyWindow) validate() error {
	if w.Location == nil {
		return ErrNilLocation
	}
	day := 24 * time.Hour
	if w.Start < 0 || w.End <= 0 || w.Start >= w.End || w.End > day {
		return ErrInvalidWindow
	}
	return nil
}

// Policy defines how a job key should be spaced and constrained over time.
type Policy struct {
	// MinGap is the minimum distance between scheduled jobs with the same key.
	MinGap time.Duration

	// Window restricts all scheduled jobs to a daily time range when non-nil.
	Window *DailyWindow

	// MaxJitter adds a random delay between 0 and MaxJitter.
	MaxJitter time.Duration
}

func (p Policy) validate() error {
	if p.MinGap < 0 {
		return ErrNegativeMinGap
	}
	if p.MaxJitter < 0 {
		return ErrNegativeJitter
	}
	if p.Window != nil {
		if err := p.Window.validate(); err != nil {
			return err
		}
	}
	return nil
}

// KeyStateStore atomically updates the most recently scheduled run time for a job key.
// Update must be atomic per key across concurrent callers.
type KeyStateStore interface {
	Update(ctx context.Context, key string, fn func(previous time.Time, exists bool) (next time.Time, err error)) (time.Time, error)
}

// EnqueueRequest describes a policy-driven enqueue operation.
type EnqueueRequest struct {
	QueueName string
	JobType   string
	JobKey    string
	Body      []byte

	// NotBefore is an optional base time. Zero value means "now" from the scheduler clock.
	NotBefore time.Time

	Policy Policy

	// Options are forwarded to queue.Enqueue (for example, priority).
	Options []jobqueue.EnqueueOption
}

// Scheduler computes policy-based run times and enqueues jobs with WithRunAt.
type Scheduler struct {
	queue jobqueue.Queue
	store KeyStateStore
	clock func() time.Time

	rngMu sync.Mutex
	rng   *rand.Rand
}

// Option configures a Scheduler.
type Option func(*Scheduler)

// WithClock overrides the clock used for zero-value NotBefore requests.
func WithClock(clock func() time.Time) Option {
	return func(s *Scheduler) {
		if clock != nil {
			s.clock = clock
		}
	}
}

// WithRandSource overrides jitter randomness.
func WithRandSource(src rand.Source) Option {
	return func(s *Scheduler) {
		if src != nil {
			s.rng = rand.New(src)
		}
	}
}

// New returns a Scheduler that uses the provided queue and key state store.
func New(queue jobqueue.Queue, store KeyStateStore, opts ...Option) (*Scheduler, error) {
	if queue == nil {
		return nil, ErrNilQueue
	}
	if store == nil {
		return nil, ErrNilStore
	}

	s := &Scheduler{
		queue: queue,
		store: store,
		clock: time.Now,
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// NextRunAt computes and reserves the next run time for the given key.
func (s *Scheduler) NextRunAt(ctx context.Context, key string, notBefore time.Time, policy Policy) (time.Time, error) {
	if strings.TrimSpace(key) == "" {
		return time.Time{}, ErrEmptyJobKey
	}
	if err := policy.validate(); err != nil {
		return time.Time{}, err
	}
	if notBefore.IsZero() {
		notBefore = s.clock()
	}

	next, err := s.store.Update(ctx, key, func(previous time.Time, exists bool) (time.Time, error) {
		base := notBefore
		if exists && policy.MinGap > 0 {
			minByGap := previous.Add(policy.MinGap)
			if minByGap.After(base) {
				base = minByGap
			}
		}

		scheduled := alignToWindow(base, policy.Window)
		scheduled = scheduled.Add(s.jitterDelta(policy.MaxJitter, policy.Window, scheduled))
		if scheduled.IsZero() {
			return time.Time{}, ErrZeroScheduledAt
		}
		return scheduled, nil
	})
	if err != nil {
		return time.Time{}, err
	}

	return next, nil
}

// Enqueue computes the next run time for request.JobKey and enqueues with WithRunAt.
func (s *Scheduler) Enqueue(ctx context.Context, request EnqueueRequest) (jobID string, runAt time.Time, err error) {
	if strings.TrimSpace(request.QueueName) == "" {
		return "", time.Time{}, ErrEmptyQueueName
	}
	if strings.TrimSpace(request.JobType) == "" {
		return "", time.Time{}, ErrEmptyJobType
	}
	if request.Body == nil {
		return "", time.Time{}, ErrNilBody
	}

	runAt, err = s.NextRunAt(ctx, request.JobKey, request.NotBefore, request.Policy)
	if err != nil {
		return "", time.Time{}, err
	}

	opts := append([]jobqueue.EnqueueOption{}, request.Options...)
	opts = append(opts, jobqueue.WithRunAt(runAt))

	jobID, err = s.queue.Enqueue(ctx, request.QueueName, request.JobType, request.Body, opts...)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("scheduler: enqueue failed: %w", err)
	}
	return jobID, runAt, nil
}

func alignToWindow(t time.Time, window *DailyWindow) time.Time {
	if window == nil {
		return t
	}

	local := t.In(window.Location)
	year, month, day := local.Date()
	dayStart := time.Date(year, month, day, 0, 0, 0, 0, window.Location)
	windowStart := dayStart.Add(window.Start)
	windowEnd := dayStart.Add(window.End)

	if local.Before(windowStart) {
		return windowStart
	}
	if !local.Before(windowEnd) {
		return windowStart.Add(24 * time.Hour)
	}
	return local
}

func (s *Scheduler) jitterDelta(maxJitter time.Duration, window *DailyWindow, base time.Time) time.Duration {
	if maxJitter <= 0 {
		return 0
	}

	limit := maxJitter
	if window != nil {
		windowEnd := endOfWindowFor(base, *window)
		remaining := windowEnd.Sub(base)
		if remaining <= 0 {
			return 0
		}
		if limit > remaining {
			limit = remaining
		}
	}
	if limit <= 0 {
		return 0
	}

	maxN := int64(limit)
	s.rngMu.Lock()
	defer s.rngMu.Unlock()
	return time.Duration(s.rng.Int63n(maxN + 1))
}

func endOfWindowFor(t time.Time, window DailyWindow) time.Time {
	local := t.In(window.Location)
	year, month, day := local.Date()
	dayStart := time.Date(year, month, day, 0, 0, 0, 0, window.Location)
	return dayStart.Add(window.End)
}
