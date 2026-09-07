package reliability

import (
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// firstLine trims s to its first line and caps its length, so a multi-line
// kubelet message does not blow up an Event or a log record.
func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	const max = 240
	if len(s) > max {
		s = s[:max] + "…"
	}
	if s == "" {
		return "(no message)"
	}
	return s
}

// roundDur rounds d to a human-friendly unit for messages.
func roundDur(d time.Duration) time.Duration {
	switch {
	case d < time.Minute:
		return d.Round(time.Second)
	case d < time.Hour:
		return d.Round(time.Minute)
	default:
		return d.Round(time.Hour)
	}
}

func typesUID(s string) types.UID { return types.UID(s) }

// fieldPathForContainer produces the spec.containers{name} field path that
// kubectl shows in "Events:" output, matching what the kubelet itself emits.
func fieldPathForContainer(name string) string {
	if name == "" {
		return ""
	}
	return "spec.containers{" + name + "}"
}
