package alert

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Mitulol/kube-reliability-sentinel/internal/reliability"
)

type countingSink struct {
	mu sync.Mutex
	n  int
}

func (c *countingSink) Fire(context.Context, reliability.Alert) {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
}
func (c *countingSink) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

type fakeCounters struct{ fired, suppressed int }

func (f *fakeCounters) AlertFired(string, string) { f.fired++ }
func (f *fakeCounters) AlertSuppressed(string)    { f.suppressed++ }
func (f *fakeCounters) KubeEventEmitted(string)   {}

func alertFixture() reliability.Alert {
	return reliability.Alert{
		Reason: reliability.ReasonCrashLoopBackOff, Severity: reliability.SeverityCritical,
		Kind: "Pod", Namespace: "default", Name: "boom", Container: "app",
	}
}

func TestDeduperSuppressesWithinResendInterval(t *testing.T) {
	next := &countingSink{}
	fc := &fakeCounters{}
	d := NewDeduper(next, 10*time.Minute, fc)

	now := time.Now()
	d.clock = func() time.Time { return now }

	a := alertFixture()
	d.Fire(context.Background(), a)
	d.Fire(context.Background(), a)
	d.Fire(context.Background(), a)

	if next.count() != 1 {
		t.Fatalf("downstream got %d alerts, want 1", next.count())
	}
	if fc.fired != 1 || fc.suppressed != 2 {
		t.Fatalf("counters: fired=%d suppressed=%d, want 1/2", fc.fired, fc.suppressed)
	}

	// Advance past the resend interval: it should pass through again.
	now = now.Add(11 * time.Minute)
	d.Fire(context.Background(), a)
	if next.count() != 2 {
		t.Fatalf("after resend interval, downstream got %d, want 2", next.count())
	}
}

func TestDeduperForgetMakesRecurrenceImmediate(t *testing.T) {
	next := &countingSink{}
	d := NewDeduper(next, time.Hour, &fakeCounters{})
	now := time.Now()
	d.clock = func() time.Time { return now }

	a := alertFixture()
	d.Fire(context.Background(), a)
	d.Forget(a.Key())
	d.Fire(context.Background(), a) // same alert, but forgotten: should pass

	if next.count() != 2 {
		t.Fatalf("downstream got %d, want 2 (Forget should reset dedup state)", next.count())
	}
}

func TestDeduperDistinctAlertsNotSuppressed(t *testing.T) {
	next := &countingSink{}
	d := NewDeduper(next, time.Hour, &fakeCounters{})

	a := alertFixture()
	b := alertFixture()
	b.Container = "sidecar"
	c := alertFixture()
	c.Reason = reliability.ReasonOOMKilled

	d.Fire(context.Background(), a)
	d.Fire(context.Background(), b)
	d.Fire(context.Background(), c)

	if next.count() != 3 {
		t.Fatalf("downstream got %d, want 3 (distinct keys)", next.count())
	}
}

func TestMultiSinkFansOut(t *testing.T) {
	a, b := &countingSink{}, &countingSink{}
	m := MultiSink{a, b}
	m.Fire(context.Background(), alertFixture())
	if a.count() != 1 || b.count() != 1 {
		t.Fatalf("fan-out failed: a=%d b=%d", a.count(), b.count())
	}
}
