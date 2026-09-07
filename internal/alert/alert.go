// Package alert delivers the alerts produced by package reliability.
//
// The pipeline is: rule engine -> Deduper (drop repeats, rate-limit) ->
// fan-out to one or more Sinks. Two sinks ship: a structured slog sink
// (JSON or text) and an EventRecorder sink that writes real Kubernetes
// Events, so alerts show up under "kubectl describe pod" / "kubectl get
// events" exactly like a first-party controller's would.
package alert

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Mitulol/kube-reliability-sentinel/internal/reliability"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
)

// Sink consumes fully-formed alerts. Implementations must be safe for
// concurrent use; the controller may reconcile several objects at once.
type Sink interface {
	Fire(ctx context.Context, a reliability.Alert)
}

// SinkFunc adapts a function to Sink.
type SinkFunc func(ctx context.Context, a reliability.Alert)

func (f SinkFunc) Fire(ctx context.Context, a reliability.Alert) { f(ctx, a) }

// MultiSink fans one alert out to every wrapped sink.
type MultiSink []Sink

func (m MultiSink) Fire(ctx context.Context, a reliability.Alert) {
	for _, s := range m {
		s.Fire(ctx, a)
	}
}

// Counters is the subset of package metrics the alert pipeline needs,
// expressed as an interface so package alert does not import package
// metrics (keeps the dependency arrows one-directional and the sinks
// unit-testable with a fake).
type Counters interface {
	AlertFired(reason, severity string)
	AlertSuppressed(reason string)
	KubeEventEmitted(reason string)
}

// Deduper wraps a downstream Sink and suppresses an alert whose Key was
// already fired within ResendInterval. This is what stops a Pod that sits
// in CrashLoopBackOff for an hour from emitting an alert on every single
// informer resync.
type Deduper struct {
	next     Sink
	resend   time.Duration
	counters Counters
	clock    func() time.Time

	mu       sync.Mutex
	lastSeen map[string]time.Time
}

// NewDeduper wraps next. resend is the minimum gap between two identical
// alerts actually reaching next.
func NewDeduper(next Sink, resend time.Duration, counters Counters) *Deduper {
	return &Deduper{
		next:     next,
		resend:   resend,
		counters: counters,
		clock:    time.Now,
		lastSeen: make(map[string]time.Time),
	}
}

// Fire passes a through to the wrapped sink unless an identical alert was
// delivered less than resend ago.
func (d *Deduper) Fire(ctx context.Context, a reliability.Alert) {
	key := a.Key()
	now := d.clock()

	d.mu.Lock()
	last, seen := d.lastSeen[key]
	if seen && now.Sub(last) < d.resend {
		d.mu.Unlock()
		if d.counters != nil {
			d.counters.AlertSuppressed(string(a.Reason))
		}
		return
	}
	d.lastSeen[key] = now
	d.mu.Unlock()

	if d.counters != nil {
		d.counters.AlertFired(string(a.Reason), string(a.Severity))
	}
	d.next.Fire(ctx, a)
}

// Forget drops the dedup record for key. The controller calls this when an
// object's alert clears, so that if the same problem recurs later it is
// reported immediately rather than being swallowed by a stale timestamp.
func (d *Deduper) Forget(key string) {
	d.mu.Lock()
	delete(d.lastSeen, key)
	d.mu.Unlock()
}

// ActiveKeys is the set of alert keys currently being tracked, used by the
// controller to reconcile "what cleared since last time".
func (d *Deduper) ActiveKeys() map[string]struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]struct{}, len(d.lastSeen))
	for k := range d.lastSeen {
		out[k] = struct{}{}
	}
	return out
}

// SlogSink writes each alert as one structured log record.
type SlogSink struct{ log *slog.Logger }

// NewSlogSink builds a SlogSink over log.
func NewSlogSink(log *slog.Logger) *SlogSink { return &SlogSink{log: log} }

func (s *SlogSink) Fire(_ context.Context, a reliability.Alert) {
	lvl := slog.LevelWarn
	if a.Severity == reliability.SeverityCritical {
		lvl = slog.LevelError
	}
	s.log.LogAttrs(context.Background(), lvl, "reliability alert",
		slog.String("reason", string(a.Reason)),
		slog.String("severity", string(a.Severity)),
		slog.String("kind", a.Kind),
		slog.String("namespace", a.Namespace),
		slog.String("name", a.Name),
		slog.String("container", a.Container),
		slog.String("message", a.Message),
	)
}

// EventSink emits a real Kubernetes Event for each alert via an
// EventRecorder. Events are namespaced to the offending object and visible
// through kubectl, which is how an operator would actually notice these in
// a cluster they do not have Sentinel's logs for.
type EventSink struct {
	recorder record.EventRecorder
	counters Counters
}

// NewEventSink builds an EventSink over recorder.
func NewEventSink(recorder record.EventRecorder, counters Counters) *EventSink {
	return &EventSink{recorder: recorder, counters: counters}
}

func (s *EventSink) Fire(_ context.Context, a reliability.Alert) {
	ref := a.ObjectReference()
	// eventType is "Warning" for everything Sentinel reports; the Event API
	// has no "Critical". Severity is preserved in the reason-adjacent
	// message and in metrics.
	s.recorder.Event(ref, corev1.EventTypeWarning, "Sentinel"+string(a.Reason), a.Message)
	if s.counters != nil {
		s.counters.KubeEventEmitted(string(a.Reason))
	}
}
