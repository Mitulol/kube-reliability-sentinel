// Package reliability is the rule engine at the core of Sentinel.
//
// Every exported function here is pure: it maps a single Kubernetes object
// (a Pod or a Node) plus a set of thresholds and a reference time to a slice
// of Alerts. There is no cluster access, no goroutines, and no shared state,
// which is what makes the rules exhaustively unit-testable against
// hand-constructed fixtures (see reliability_test.go) without ever standing
// up an API server.
//
// Stateful detection that needs history across observations (rapid restart
// bursts) lives in package watcher, not here, precisely so this package can
// stay pure.
package reliability

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Severity is the urgency of an alert. It maps to the Kubernetes Event type
// ("Normal"/"Warning") and is exposed as a Prometheus label.
type Severity string

const (
	SeverityWarning  Severity = "Warning"
	SeverityCritical Severity = "Critical"
)

// EventType returns the corev1 Event type string for this severity. The
// Kubernetes Event API only has "Normal" and "Warning", so both Sentinel
// severities map to "Warning" there while staying distinct in logs/metrics.
func (s Severity) EventType() string { return "Warning" }

// Reason is a stable, machine-readable classification of a reliability
// problem. The zero value is invalid. These strings double as Kubernetes
// Event reasons and Prometheus label values, so they must stay stable.
type Reason string

const (
	ReasonCrashLoopBackOff Reason = "CrashLoopBackOff"
	ReasonOOMKilled        Reason = "OOMKilled"
	ReasonImagePullError   Reason = "ImagePullError"
	ReasonHighRestartCount Reason = "HighRestartCount"
	ReasonRapidRestart     Reason = "RapidRestart"
	ReasonPodStuckPending  Reason = "PodStuckPending"
	ReasonNodeNotReady     Reason = "NodeNotReady"
	ReasonNodePressure     Reason = "NodePressure"
)

// Alert is one detected reliability problem for one object. It carries
// enough identity to build a corev1.ObjectReference for a real Kubernetes
// Event and to deduplicate against prior firings.
type Alert struct {
	Reason    Reason
	Severity  Severity
	Kind      string // "Pod" or "Node"
	Namespace string // empty for cluster-scoped objects (Nodes)
	Name      string
	UID       string
	Container string // set when the problem is specific to one container
	Message   string
}

// Key is the deduplication identity of an alert: the same problem on the
// same object/container collapses to one key regardless of how many times
// it is observed.
func (a Alert) Key() string {
	return fmt.Sprintf("%s|%s|%s/%s|%s", a.Reason, a.Kind, a.Namespace, a.Name, a.Container)
}

// ObjectReference builds the reference a record.EventRecorder needs to
// attach an Event to the offending object.
func (a Alert) ObjectReference() *corev1.ObjectReference {
	ref := &corev1.ObjectReference{
		Kind:      a.Kind,
		Namespace: a.Namespace,
		Name:      a.Name,
		UID:       typesUID(a.UID),
	}
	if a.Kind == "Pod" {
		ref.APIVersion = "v1"
		ref.FieldPath = fieldPathForContainer(a.Container)
	}
	if a.Kind == "Node" {
		ref.APIVersion = "v1"
	}
	return ref
}

// Thresholds holds every tunable knob for the rule engine. Construct via
// DefaultThresholds and override individual fields from flags.
type Thresholds struct {
	// RestartCountWarn is the per-container restart count at or above which
	// HighRestartCount fires, provided the container is not currently Ready.
	RestartCountWarn int32
	// PendingGracePeriod is how long a Pod may stay in Pending (measured
	// from its creation timestamp) before PodStuckPending fires.
	PendingGracePeriod time.Duration
	// NodeNotReadyGracePeriod is how long a Node's Ready condition may be
	// non-True (measured from the condition's last transition) before
	// NodeNotReady fires. This absorbs brief kubelet heartbeat blips.
	NodeNotReadyGracePeriod time.Duration
}

// DefaultThresholds returns the values used when no flags override them.
func DefaultThresholds() Thresholds {
	return Thresholds{
		RestartCountWarn:        5,
		PendingGracePeriod:      5 * time.Minute,
		NodeNotReadyGracePeriod: 2 * time.Minute,
	}
}

// EvaluatePod returns every alert that currently applies to pod. now is
// injected rather than read from the clock so tests are deterministic.
//
// A Pod that has recovered (container back to Ready, no waiting reason,
// phase Running) produces no alerts; the alert sink's resend logic is what
// stops an already-fired alert from repeating, and returning nothing here
// is what lets a resolved alert age out.
func EvaluatePod(pod *corev1.Pod, t Thresholds, now time.Time) []Alert {
	if pod == nil || pod.DeletionTimestamp != nil {
		// A Pod being torn down is expected to churn; don't alert on it.
		return nil
	}

	var alerts []Alert
	base := Alert{
		Kind:      "Pod",
		Namespace: pod.Namespace,
		Name:      pod.Name,
		UID:       string(pod.UID),
	}

	sawContainerWaitingAlert := false
	for _, cs := range pod.Status.ContainerStatuses {
		c := base
		c.Container = cs.Name

		// CrashLoopBackOff: the kubelet is deliberately backing off between
		// restart attempts because the container keeps exiting.
		if w := cs.State.Waiting; w != nil && w.Reason == "CrashLoopBackOff" {
			a := c
			a.Reason = ReasonCrashLoopBackOff
			a.Severity = SeverityCritical
			a.Message = fmt.Sprintf("Container %q in CrashLoopBackOff after %d restarts: %s",
				cs.Name, cs.RestartCount, firstLine(w.Message))
			alerts = append(alerts, a)
			sawContainerWaitingAlert = true
		}

		// Image pull failures: distinct from a crash loop; the container
		// never started. Lower severity because it is usually a bad tag or
		// missing pull secret, not a production incident.
		if w := cs.State.Waiting; w != nil && (w.Reason == "ImagePullBackOff" || w.Reason == "ErrImagePull") {
			a := c
			a.Reason = ReasonImagePullError
			a.Severity = SeverityWarning
			a.Message = fmt.Sprintf("Container %q cannot pull its image (%s): %s",
				cs.Name, w.Reason, firstLine(w.Message))
			alerts = append(alerts, a)
			sawContainerWaitingAlert = true
		}

		// OOMKilled: check both the current and previous termination so a
		// container that OOMed, restarted, and is now running is still
		// surfaced once.
		if term := oomTermination(cs); term != nil {
			a := c
			a.Reason = ReasonOOMKilled
			a.Severity = SeverityCritical
			a.Message = fmt.Sprintf("Container %q was OOMKilled (exit code %d) after %d restarts",
				cs.Name, term.ExitCode, cs.RestartCount)
			alerts = append(alerts, a)
		}

		// High restart count, gated on the container not currently being
		// Ready. Without that gate a container that crashed 20 times last
		// week but is healthy now would alert forever.
		if cs.RestartCount >= t.RestartCountWarn && !cs.Ready {
			a := c
			a.Reason = ReasonHighRestartCount
			a.Severity = SeverityWarning
			a.Message = fmt.Sprintf("Container %q has restarted %d times (>= %d) and is not Ready",
				cs.Name, cs.RestartCount, t.RestartCountWarn)
			alerts = append(alerts, a)
		}
	}

	// Pending-too-long, measured from creation so a Pod that legitimately
	// takes 20s to schedule does not trip a 5m threshold. Suppressed when a
	// container already carries a specific waiting reason (image pull, etc.)
	// since that more precise alert is the actionable one.
	if pod.Status.Phase == corev1.PodPending && !sawContainerWaitingAlert {
		age := now.Sub(pod.CreationTimestamp.Time)
		if age >= t.PendingGracePeriod {
			a := base
			a.Reason = ReasonPodStuckPending
			a.Severity = SeverityWarning
			a.Message = fmt.Sprintf("Pod has been Pending for %s (>= %s); %s",
				roundDur(age), t.PendingGracePeriod, pendingHint(pod))
			alerts = append(alerts, a)
		}
	}

	return alerts
}

// EvaluateNode returns every alert that currently applies to node.
func EvaluateNode(node *corev1.Node, t Thresholds, now time.Time) []Alert {
	if node == nil {
		return nil
	}
	var alerts []Alert
	base := Alert{Kind: "Node", Name: node.Name, UID: string(node.UID)}

	for _, cond := range node.Status.Conditions {
		switch cond.Type {
		case corev1.NodeReady:
			if cond.Status != corev1.ConditionTrue {
				since := now.Sub(cond.LastTransitionTime.Time)
				if since >= t.NodeNotReadyGracePeriod {
					a := base
					a.Reason = ReasonNodeNotReady
					a.Severity = SeverityCritical
					a.Message = fmt.Sprintf("Node Ready=%s for %s (>= %s): %s",
						cond.Status, roundDur(since), t.NodeNotReadyGracePeriod, firstLine(cond.Message))
					alerts = append(alerts, a)
				}
			}
		case corev1.NodeMemoryPressure, corev1.NodeDiskPressure, corev1.NodePIDPressure:
			if cond.Status == corev1.ConditionTrue {
				a := base
				a.Reason = ReasonNodePressure
				a.Severity = SeverityWarning
				a.Message = fmt.Sprintf("Node reports %s=True: %s", cond.Type, firstLine(cond.Message))
				alerts = append(alerts, a)
			}
		}
	}
	return alerts
}

// oomTermination returns the ContainerStateTerminated that represents an OOM
// kill for cs, checking the live state first and then the previous one, or
// nil if the container was not OOMKilled.
func oomTermination(cs corev1.ContainerStatus) *corev1.ContainerStateTerminated {
	if s := cs.State.Terminated; s != nil && s.Reason == "OOMKilled" {
		return s
	}
	if s := cs.LastTerminationState.Terminated; s != nil && s.Reason == "OOMKilled" {
		return s
	}
	return nil
}

// pendingHint tries to explain why a Pod is stuck by reading its
// PodScheduled condition, the single most common cause (unsatisfiable
// resource requests, taints, no matching node).
func pendingHint(pod *corev1.Pod) string {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status != corev1.ConditionTrue {
			if c.Message != "" {
				return "not scheduled: " + firstLine(c.Message)
			}
			return "not scheduled (" + c.Reason + ")"
		}
	}
	return "still Pending"
}
