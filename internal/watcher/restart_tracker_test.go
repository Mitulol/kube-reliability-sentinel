package watcher

import (
	"testing"
	"time"
)

func TestRestartTrackerBurstDetection(t *testing.T) {
	tr := newRestartTracker(time.Minute, 3)
	base := time.Now()

	// Three restarts in 40s: a burst.
	if burst, _ := tr.observe("ns/pod/app", 1, base); burst {
		t.Fatal("one restart should not be a burst")
	}
	if burst, _ := tr.observe("ns/pod/app", 2, base.Add(15*time.Second)); burst {
		t.Fatal("two restarts should not be a burst")
	}
	burst, n := tr.observe("ns/pod/app", 3, base.Add(40*time.Second))
	if !burst || n != 3 {
		t.Fatalf("three restarts in 40s should be a burst (n=%d)", n)
	}
}

func TestRestartTrackerWindowExpiry(t *testing.T) {
	tr := newRestartTracker(time.Minute, 3)
	base := time.Now()

	tr.observe("k", 1, base)
	tr.observe("k", 2, base.Add(30*time.Second))
	// Third increment is 2 minutes after the first: the first two have
	// aged out of the 1-minute window.
	burst, n := tr.observe("k", 3, base.Add(2*time.Minute))
	if burst {
		t.Fatalf("increments outside the window should not count (n=%d)", n)
	}
	if n != 1 {
		t.Fatalf("only the most recent increment should remain, got n=%d", n)
	}
}

func TestRestartTrackerIgnoresStableCount(t *testing.T) {
	tr := newRestartTracker(time.Minute, 2)
	base := time.Now()
	tr.observe("k", 5, base)
	// Same count re-observed repeatedly (informer resync) must not register
	// as restarts.
	for i := 0; i < 10; i++ {
		if burst, _ := tr.observe("k", 5, base.Add(time.Duration(i)*time.Second)); burst {
			t.Fatal("re-observing a stable restart count must not trip the detector")
		}
	}
}

func TestRestartTrackerForgetPod(t *testing.T) {
	tr := newRestartTracker(time.Minute, 1)
	base := time.Now()
	tr.observe("default/web/app", 1, base)
	tr.observe("default/web/sidecar", 1, base)
	tr.observe("default/other/app", 1, base)

	tr.forgetPod("default/web")
	if _, ok := tr.seen["default/web/app"]; ok {
		t.Fatal("forgetPod should have removed default/web/app")
	}
	if _, ok := tr.seen["default/web/sidecar"]; ok {
		t.Fatal("forgetPod should have removed default/web/sidecar")
	}
	if _, ok := tr.seen["default/other/app"]; !ok {
		t.Fatal("forgetPod must not touch a different pod")
	}
}
