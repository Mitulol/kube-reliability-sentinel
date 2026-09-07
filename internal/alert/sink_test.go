package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/Mitulol/kube-reliability-sentinel/internal/reliability"
	"k8s.io/client-go/tools/record"
)

func TestSlogSinkEmitsStructuredRecord(t *testing.T) {
	var buf bytes.Buffer
	sink := NewSlogSink(slog.New(slog.NewJSONHandler(&buf, nil)))

	sink.Fire(context.Background(), reliability.Alert{
		Reason: reliability.ReasonOOMKilled, Severity: reliability.SeverityCritical,
		Kind: "Pod", Namespace: "prod", Name: "api-7d", Container: "api",
		Message: "Container \"api\" was OOMKilled (exit code 137)",
	})

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("log line is not valid JSON: %v", err)
	}
	if rec["reason"] != "OOMKilled" || rec["namespace"] != "prod" || rec["container"] != "api" {
		t.Fatalf("structured fields wrong: %v", rec)
	}
	if rec["level"] != "ERROR" {
		t.Fatalf("critical alert should log at ERROR, got %v", rec["level"])
	}
}

func TestEventSinkEmitsKubernetesEvent(t *testing.T) {
	rec := record.NewFakeRecorder(8)
	ec := &eventCounter{}
	sink := NewEventSink(rec, ec)

	sink.Fire(context.Background(), reliability.Alert{
		Reason: reliability.ReasonCrashLoopBackOff, Severity: reliability.SeverityCritical,
		Kind: "Pod", Namespace: "default", Name: "boom", UID: "u1", Container: "app",
		Message: "Container \"app\" in CrashLoopBackOff after 5 restarts",
	})

	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "SentinelCrashLoopBackOff") {
			t.Fatalf("event reason missing: %q", ev)
		}
		if !strings.Contains(ev, "Warning") {
			t.Fatalf("event type should be Warning: %q", ev)
		}
	default:
		t.Fatal("no Kubernetes Event was recorded")
	}
	if ec.emitted != 1 {
		t.Fatalf("KubeEventEmitted count = %d, want 1", ec.emitted)
	}
}

type eventCounter struct{ emitted int }

func (e *eventCounter) AlertFired(string, string) {}
func (e *eventCounter) AlertSuppressed(string)    {}
func (e *eventCounter) KubeEventEmitted(string)   { e.emitted++ }
