package reliability

import (
	"sort"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var refTime = time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)

// podBuilder keeps the test fixtures readable: each test constructs only the
// fields its rule actually reads.
type podBuilder struct{ p corev1.Pod }

func newPod(name string) *podBuilder {
	return &podBuilder{p: corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "default",
			UID:               types.UID("uid-" + name),
			CreationTimestamp: metav1.NewTime(refTime.Add(-time.Hour)),
		},
	}}
}

func (b *podBuilder) phase(p corev1.PodPhase) *podBuilder { b.p.Status.Phase = p; return b }
func (b *podBuilder) created(at time.Time) *podBuilder {
	b.p.CreationTimestamp = metav1.NewTime(at)
	return b
}
func (b *podBuilder) deleting() *podBuilder {
	t := metav1.NewTime(refTime)
	b.p.DeletionTimestamp = &t
	return b
}
func (b *podBuilder) scheduledCond(status corev1.ConditionStatus, msg string) *podBuilder {
	b.p.Status.Conditions = append(b.p.Status.Conditions, corev1.PodCondition{
		Type: corev1.PodScheduled, Status: status, Reason: "Unschedulable", Message: msg,
	})
	return b
}
func (b *podBuilder) container(cs corev1.ContainerStatus) *podBuilder {
	b.p.Status.ContainerStatuses = append(b.p.Status.ContainerStatuses, cs)
	return b
}
func (b *podBuilder) build() *corev1.Pod { return &b.p }

func waiting(name, reason string, restarts int32, ready bool) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name: name, RestartCount: restarts, Ready: ready,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason, Message: reason + " detail"}},
	}
}

func running(name string, restarts int32, ready bool) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name: name, RestartCount: restarts, Ready: ready,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(refTime)}},
	}
}

func lastOOM(name string, restarts int32, ready bool) corev1.ContainerStatus {
	cs := running(name, restarts, ready)
	cs.LastTerminationState = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
		Reason: "OOMKilled", ExitCode: 137,
	}}
	return cs
}

func reasons(alerts []Alert) []Reason {
	out := make([]Reason, 0, len(alerts))
	for _, a := range alerts {
		out = append(out, a.Reason)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func TestEvaluatePod(t *testing.T) {
	th := DefaultThresholds()

	tests := []struct {
		name string
		pod  *corev1.Pod
		want []Reason
	}{
		{
			name: "healthy running pod produces nothing",
			pod:  newPod("ok").phase(corev1.PodRunning).container(running("app", 0, true)).build(),
			want: nil,
		},
		{
			name: "crashloop is critical",
			pod:  newPod("crash").phase(corev1.PodRunning).container(waiting("app", "CrashLoopBackOff", 3, false)).build(),
			want: []Reason{ReasonCrashLoopBackOff},
		},
		{
			name: "crashloop past restart threshold fires both rules",
			pod:  newPod("crash").phase(corev1.PodRunning).container(waiting("app", "CrashLoopBackOff", 7, false)).build(),
			want: []Reason{ReasonCrashLoopBackOff, ReasonHighRestartCount},
		},
		{
			name: "image pull backoff is its own warning, not a crash loop",
			pod:  newPod("badimg").phase(corev1.PodPending).container(waiting("app", "ImagePullBackOff", 0, false)).build(),
			want: []Reason{ReasonImagePullError},
		},
		{
			name: "oomkilled detected from last termination state after restart",
			pod:  newPod("oom").phase(corev1.PodRunning).container(lastOOM("app", 2, true)).build(),
			want: []Reason{ReasonOOMKilled},
		},
		{
			name: "restarting but below threshold does not fire HighRestartCount",
			pod:  newPod("young").phase(corev1.PodRunning).container(running("app", 4, false)).build(),
			want: nil,
		},
		{
			name: "high restart count but container recovered (Ready) does not fire",
			pod:  newPod("recovered").phase(corev1.PodRunning).container(running("app", 12, true)).build(),
			want: nil,
		},
		{
			name: "high restart count while not ready fires",
			pod:  newPod("sick").phase(corev1.PodRunning).container(running("app", 12, false)).build(),
			want: []Reason{ReasonHighRestartCount},
		},
		{
			name: "pending past grace period fires",
			pod:  newPod("stuck").phase(corev1.PodPending).created(refTime.Add(-10*time.Minute)).scheduledCond(corev1.ConditionFalse, "0/3 nodes are available: insufficient memory").build(),
			want: []Reason{ReasonPodStuckPending},
		},
		{
			name: "pending within grace period does not fire",
			pod:  newPod("fresh").phase(corev1.PodPending).created(refTime.Add(-30 * time.Second)).build(),
			want: nil,
		},
		{
			name: "pod being deleted is never alerted on",
			pod:  newPod("dying").phase(corev1.PodRunning).deleting().container(waiting("app", "CrashLoopBackOff", 9, false)).build(),
			want: nil,
		},
		{
			name: "multi-container pod surfaces per-container problems",
			pod: newPod("multi").phase(corev1.PodRunning).
				container(running("sidecar", 0, true)).
				container(waiting("app", "CrashLoopBackOff", 2, false)).build(),
			want: []Reason{ReasonCrashLoopBackOff},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := reasons(EvaluatePod(tc.pod, th, refTime))
			if !equalReasons(got, tc.want) {
				t.Fatalf("reasons = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEvaluatePodAlertIdentity(t *testing.T) {
	pod := newPod("crash").phase(corev1.PodRunning).container(waiting("app", "CrashLoopBackOff", 3, false)).build()
	alerts := EvaluatePod(pod, DefaultThresholds(), refTime)
	if len(alerts) != 1 {
		t.Fatalf("want 1 alert, got %d", len(alerts))
	}
	a := alerts[0]
	if a.Severity != SeverityCritical {
		t.Errorf("severity = %s, want Critical", a.Severity)
	}
	if a.Container != "app" {
		t.Errorf("container = %q, want app", a.Container)
	}
	want := "CrashLoopBackOff|Pod|default/crash|app"
	if a.Key() != want {
		t.Errorf("key = %q, want %q", a.Key(), want)
	}
	ref := a.ObjectReference()
	if ref.FieldPath != "spec.containers{app}" {
		t.Errorf("fieldPath = %q, want spec.containers{app}", ref.FieldPath)
	}
	if string(ref.UID) != "uid-crash" {
		t.Errorf("uid = %q", ref.UID)
	}
}

func node(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("nuid-" + name)}}
}

func withCond(n *corev1.Node, ct corev1.NodeConditionType, status corev1.ConditionStatus, transitioned time.Time, msg string) *corev1.Node {
	n.Status.Conditions = append(n.Status.Conditions, corev1.NodeCondition{
		Type: ct, Status: status, LastTransitionTime: metav1.NewTime(transitioned), Message: msg,
	})
	return n
}

func TestEvaluateNode(t *testing.T) {
	th := DefaultThresholds()

	tests := []struct {
		name string
		node *corev1.Node
		want []Reason
	}{
		{
			name: "ready node produces nothing",
			node: withCond(node("n1"), corev1.NodeReady, corev1.ConditionTrue, refTime.Add(-time.Hour), ""),
			want: nil,
		},
		{
			name: "not-ready past grace fires critical",
			node: withCond(node("n2"), corev1.NodeReady, corev1.ConditionFalse, refTime.Add(-5*time.Minute), "kubelet stopped posting status"),
			want: []Reason{ReasonNodeNotReady},
		},
		{
			name: "not-ready within grace does not fire yet",
			node: withCond(node("n3"), corev1.NodeReady, corev1.ConditionUnknown, refTime.Add(-30*time.Second), ""),
			want: nil,
		},
		{
			name: "memory pressure fires a warning",
			node: withCond(withCond(node("n4"), corev1.NodeReady, corev1.ConditionTrue, refTime.Add(-time.Hour), ""),
				corev1.NodeMemoryPressure, corev1.ConditionTrue, refTime, "node has memory pressure"),
			want: []Reason{ReasonNodePressure},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := reasons(EvaluateNode(tc.node, th, refTime))
			if !equalReasons(got, tc.want) {
				t.Fatalf("reasons = %v, want %v", got, tc.want)
			}
		})
	}
}

func equalReasons(a, b []Reason) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
