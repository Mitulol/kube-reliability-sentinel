package metrics

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandlerServesMetricsAndProbes(t *testing.T) {
	m := New("v-test", "abc123")
	m.AlertFired("CrashLoopBackOff", "Critical")
	m.AlertSuppressed("CrashLoopBackOff")
	m.ObserveReconcile("Pod", 3*time.Millisecond, nil)
	m.ObserveReconcile("Pod", 1*time.Millisecond, errors.New("boom"))

	h := m.Handler()

	// /readyz is 503 until caches sync.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/readyz", nil))
	if rr.Code != 503 {
		t.Fatalf("/readyz before ready = %d, want 503", rr.Code)
	}
	m.SetReady(true)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/readyz", nil))
	if rr.Code != 200 {
		t.Fatalf("/readyz after ready = %d, want 200", rr.Code)
	}

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rr.Body)
	text := string(body)

	for _, want := range []string{
		`sentinel_alerts_fired_total{reason="CrashLoopBackOff",severity="Critical"} 1`,
		`sentinel_alerts_suppressed_total{reason="CrashLoopBackOff"} 1`,
		`sentinel_reconcile_total{kind="Pod"} 2`,
		`sentinel_reconcile_errors_total{kind="Pod"} 1`,
		`sentinel_build_info{`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("/metrics output missing %q", want)
		}
	}
}

func TestServeShutsDownOnContextCancel(t *testing.T) {
	m := New("v", "c")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Serve(ctx, "127.0.0.1:0") }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned %v, want nil on clean shutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after context cancel")
	}
}
