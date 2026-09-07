// Package k8sclient is a minimal, dependency-free client for the parts of the
// Kubernetes API this project needs (List + Watch on Pods). It intentionally
// avoids k8s.io/client-go: this environment's network egress does not allow
// pulling Go modules (only a small allowlist of hosts is reachable), and
// hand-rolling the REST/watch protocol against the stdlib is also a more
// direct demonstration of understanding how the underlying API actually
// works (list-then-watch, resourceVersion bookmarks, 410 Gone on an
// expired watch requiring a relist) rather than delegating it to a library.
//
// Only the JSON fields Sentinel actually reads are modeled below; this is a
// deliberate subset of the real Kubernetes API types, not the full schema.
package k8sclient

import (
	"encoding/json"
	"time"
)

// ObjectMeta mirrors the subset of metav1.ObjectMeta this project reads.
// CreationTimestamp is RFC3339 in the real API, which encoding/json's
// time.Time already knows how to unmarshal without a custom decoder.
type ObjectMeta struct {
	Name              string    `json:"name"`
	Namespace         string    `json:"namespace"`
	UID               string    `json:"uid"`
	ResourceVersion   string    `json:"resourceVersion"`
	CreationTimestamp time.Time `json:"creationTimestamp"`
}

// ContainerStateWaiting mirrors corev1.ContainerStateWaiting.
type ContainerStateWaiting struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// ContainerStateTerminated mirrors corev1.ContainerStateTerminated.
type ContainerStateTerminated struct {
	Reason   string `json:"reason"`
	ExitCode int32  `json:"exitCode"`
}

// ContainerState mirrors corev1.ContainerState: exactly one field is set,
// same as the real API.
type ContainerState struct {
	Waiting    *ContainerStateWaiting    `json:"waiting,omitempty"`
	Terminated *ContainerStateTerminated `json:"terminated,omitempty"`
}

// ContainerStatus mirrors the subset of corev1.ContainerStatus this project
// needs to detect crash loops and OOM kills.
type ContainerStatus struct {
	Name         string         `json:"name"`
	RestartCount int32          `json:"restartCount"`
	State        ContainerState `json:"state"`
	LastState    ContainerState `json:"lastState"`
	Ready        bool           `json:"ready"`
}

// PodStatus mirrors the subset of corev1.PodStatus this project reads.
type PodStatus struct {
	Phase             string            `json:"phase"`
	ContainerStatuses []ContainerStatus `json:"containerStatuses"`
}

// Pod mirrors the subset of corev1.Pod this project reads.
type Pod struct {
	Metadata ObjectMeta `json:"metadata"`
	Status   PodStatus  `json:"status"`
}

// Key returns the cache key ("namespace/name") for a Pod.
func (p *Pod) Key() string {
	return p.Metadata.Namespace + "/" + p.Metadata.Name
}

// PodList mirrors corev1.PodList as returned by a plain (non-watch) LIST call.
type PodList struct {
	Metadata struct {
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Items []Pod `json:"items"`
}

// WatchEventType mirrors the "type" field of a Kubernetes watch event.
type WatchEventType string

const (
	EventAdded    WatchEventType = "ADDED"
	EventModified WatchEventType = "MODIFIED"
	EventDeleted  WatchEventType = "DELETED"
	EventError    WatchEventType = "ERROR"
	EventBookmark WatchEventType = "BOOKMARK"
)

// Status mirrors metav1.Status, the object the API server sends as the
// "object" field of a watch event when Type == EventError (for example a
// 410 Gone when the requested resourceVersion has been compacted away).
type Status struct {
	Code    int    `json:"code"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// WatchEvent mirrors the newline-delimited JSON objects the API server
// streams back on a `?watch=true` request: {"type": "...", "object": {...}}.
// Object is left raw because its schema depends on Type: a Pod for
// ADDED/MODIFIED/DELETED, a Status for ERROR.
type WatchEvent struct {
	Type   WatchEventType  `json:"type"`
	Object json.RawMessage `json:"object"`
}

// AsPod decodes Object as a Pod. Callers should only call this when Type is
// ADDED, MODIFIED, DELETED or BOOKMARK.
func (e WatchEvent) AsPod() (Pod, error) {
	var p Pod
	err := json.Unmarshal(e.Object, &p)
	return p, err
}

// AsStatus decodes Object as a Status. Callers should only call this when
// Type is ERROR.
func (e WatchEvent) AsStatus() (Status, error) {
	var s Status
	err := json.Unmarshal(e.Object, &s)
	return s, err
}
