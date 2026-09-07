package watcher

import (
	"sync"
	"time"
)

// restartTracker turns the point-in-time restartCount that the API exposes
// into a rate signal. The rule engine (package reliability) is pure and
// sees one object at a time, so "this container restarted 4 times in the
// last 2 minutes" can only be detected by something that remembers prior
// observations. That is this type, and it is the reason the controller
// keeps local state at all.
type restartTracker struct {
	window    time.Duration
	threshold int

	mu   sync.Mutex
	seen map[string]containerHistory
}

type containerHistory struct {
	lastCount  int32
	increments []time.Time // timestamps of observed restartCount increases
}

func newRestartTracker(window time.Duration, threshold int) *restartTracker {
	return &restartTracker{
		window:    window,
		threshold: threshold,
		seen:      make(map[string]containerHistory),
	}
}

// observe records that container key currently reports count restarts as of
// now. It returns whether a rapid-restart burst is active (>= threshold
// increments within window) and how many increments are in the window.
func (r *restartTracker) observe(key string, count int32, now time.Time) (burst bool, inWindow int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	h := r.seen[key]
	if count > h.lastCount {
		// Attribute one increment event per observation even if the count
		// jumped by more than one between watch updates; we care about the
		// cadence of observations, not exact restart arithmetic.
		h.increments = append(h.increments, now)
	}
	h.lastCount = count

	cutoff := now.Add(-r.window)
	kept := h.increments[:0]
	for _, t := range h.increments {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	h.increments = kept
	r.seen[key] = h

	return len(kept) >= r.threshold, len(kept)
}

// forget drops history for a container (its Pod was deleted).
func (r *restartTracker) forget(key string) {
	r.mu.Lock()
	delete(r.seen, key)
	r.mu.Unlock()
}

// forgetPod drops history for every container belonging to podKey
// ("namespace/name").
func (r *restartTracker) forgetPod(podKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k := range r.seen {
		if podContainerKeyMatchesPod(k, podKey) {
			delete(r.seen, k)
		}
	}
}
