package watcher

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Mitulol/kube-reliability-sentinel/internal/alert"
	"github.com/Mitulol/kube-reliability-sentinel/internal/metrics"
	"github.com/Mitulol/kube-reliability-sentinel/internal/reliability"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func toRuntimeObjects(in []any) []runtime.Object {
	out := make([]runtime.Object, 0, len(in))
	for _, o := range in {
		out = append(out, o.(runtime.Object))
	}
	return out
}

// captureSink records every alert the pipeline delivers.
type captureSink struct {
	mu     sync.Mutex
	alerts []reliability.Alert
	ch     chan reliability.Alert
}

func newCaptureSink() *captureSink {
	return &captureSink{ch: make(chan reliability.Alert, 64)}
}

func (c *captureSink) Fire(_ context.Context, a reliability.Alert) {
	c.mu.Lock()
	c.alerts = append(c.alerts, a)
	c.mu.Unlock()
	select {
	case c.ch <- a:
	default:
	}
}

func (c *captureSink) reasonsSeen() map[reliability.Reason]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[reliability.Reason]int{}
	for _, a := range c.alerts {
		out[a.Reason]++
	}
	return out
}

// waitForReason blocks until an alert with reason is delivered or the
// deadline passes.
func (c *captureSink) waitForReason(t *testing.T, reason reliability.Reason, within time.Duration) reliability.Alert {
	t.Helper()
	deadline := time.After(within)
	for {
		select {
		case a := <-c.ch:
			if a.Reason == reason {
				return a
			}
		case <-deadline:
			t.Fatalf("timed out after %s waiting for %s; saw %v", within, reason, c.reasonsSeen())
		}
	}
}

func testController(t *testing.T, objects ...any) (*Controller, *captureSink, *fake.Clientset) {
	t.Helper()
	runtimeObjs := make([]any, 0, len(objects))
	runtimeObjs = append(runtimeObjs, objects...)
	client := fake.NewSimpleClientset(toRuntimeObjects(runtimeObjs)...)

	sink := newCaptureSink()
	m := metrics.New("test", "test")
	dd := alert.NewDeduper(sink, time.Hour, m)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctrl := New(client, Options{
		Namespace:          "",
		Thresholds:         reliability.DefaultThresholds(),
		WatchNodes:         true,
		ResyncPeriod:       time.Hour, // no automatic resync during the test
		RapidRestartWindow: time.Minute,
		RapidRestartCount:  3,
		ClearGracePeriod:   150 * time.Millisecond,
		Workers:            2,
	}, dd, dd, m, log)
	return ctrl, sink, client
}

func TestControllerDetectsCrashLoop(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "boom", Namespace: "default", UID: "u1"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "app", RestartCount: 4, Ready: false,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason: "CrashLoopBackOff", Message: "back-off 40s restarting failed container",
				}},
			}},
		},
	}
	ctrl, sink, _ := testController(t, pod)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ctrl.Run(ctx) }()

	a := sink.waitForReason(t, reliability.ReasonCrashLoopBackOff, 5*time.Second)
	if a.Namespace != "default" || a.Name != "boom" || a.Container != "app" {
		t.Fatalf("unexpected alert identity: %+v", a)
	}
	if a.Severity != reliability.SeverityCritical {
		t.Fatalf("severity = %s, want Critical", a.Severity)
	}
}

func TestControllerReactsToUpdatesAndRecovery(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "flaky", Namespace: "default", UID: "u2"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "app", RestartCount: 0, Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
	ctrl, sink, client := testController(t, pod)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ctrl.Run(ctx) }()

	// Wait for the initial healthy reconcile to have happened.
	time.Sleep(200 * time.Millisecond)
	if got := sink.reasonsSeen(); len(got) != 0 {
		t.Fatalf("expected no alerts for healthy pod, got %v", got)
	}

	// Push it into OOMKilled via an update.
	pod.Status.ContainerStatuses[0].LastTerminationState = corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137},
	}
	pod.Status.ContainerStatuses[0].RestartCount = 1
	if _, err := client.CoreV1().Pods("default").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	sink.waitForReason(t, reliability.ReasonOOMKilled, 5*time.Second)

	// Recover: clear the termination record. The first reconcile after
	// recovery puts the alert into its clear grace period; it is not
	// forgotten yet.
	pod.Status.ContainerStatuses[0].LastTerminationState = corev1.ContainerState{}
	if _, err := client.CoreV1().Pods("default").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	ctrl.mu.Lock()
	st, ok := ctrl.active["Pod|default/flaky"]
	var clearing bool
	for _, s := range st {
		clearing = !s.clearingSince.IsZero()
	}
	ctrl.mu.Unlock()
	if !ok || !clearing {
		t.Fatalf("expected recovered pod's alert to be in the clear grace period, active=%v", st)
	}

	// A second reconcile after the grace period elapses actually forgets it.
	time.Sleep(200 * time.Millisecond)
	pod.Labels = map[string]string{"touch": "2"}
	if _, err := client.CoreV1().Pods("default").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	ctrl.mu.Lock()
	_, stillActive := ctrl.active["Pod|default/flaky"]
	ctrl.mu.Unlock()
	if stillActive {
		t.Fatal("expected recovered pod to be cleared from the active set after the grace period")
	}
}

// A crashlooping container flaps between Waiting=CrashLoopBackOff and
// Running on every restart. That must not defeat the deduper: the alert
// should reach the sink once, not once per flap.
func TestControllerCrashLoopFlapDoesNotReAlert(t *testing.T) {
	waitingCS := corev1.ContainerStatus{
		Name: "app", RestartCount: 2, Ready: false,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
	}
	runningCS := corev1.ContainerStatus{
		Name: "app", RestartCount: 2, Ready: false,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "flap", Namespace: "default", UID: "u3"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{waitingCS}},
	}
	ctrl, sink, client := testController(t, pod)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ctrl.Run(ctx) }()

	sink.waitForReason(t, reliability.ReasonCrashLoopBackOff, 5*time.Second)

	// Flap Running <-> Waiting several times, faster than the 150ms grace.
	for i := 0; i < 6; i++ {
		if i%2 == 0 {
			pod.Status.ContainerStatuses[0] = runningCS
		} else {
			pod.Status.ContainerStatuses[0] = waitingCS
		}
		if _, err := client.CoreV1().Pods("default").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(40 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)

	if n := sink.reasonsSeen()[reliability.ReasonCrashLoopBackOff]; n != 1 {
		t.Fatalf("CrashLoopBackOff delivered %d times through the deduper, want 1", n)
	}
}

func TestControllerDetectsNodeNotReady(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1", UID: "n1"},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: corev1.ConditionFalse,
			LastTransitionTime: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
			Message:            "kubelet is posting NotReady",
		}}},
	}
	ctrl, sink, _ := testController(t, node)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ctrl.Run(ctx) }()

	a := sink.waitForReason(t, reliability.ReasonNodeNotReady, 5*time.Second)
	if a.Kind != "Node" || a.Name != "worker-1" {
		t.Fatalf("unexpected node alert: %+v", a)
	}
}
