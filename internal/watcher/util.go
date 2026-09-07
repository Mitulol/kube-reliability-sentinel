package watcher

import (
	"strings"

	"k8s.io/client-go/tools/cache"
)

// metaNamespaceKey extracts "namespace/name" (or "name") from an informer
// object, transparently unwrapping the DeletedFinalStateUnknown tombstone
// that a Delete event carries after a missed watch.
func metaNamespaceKey(obj any) (string, bool) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		return "", false
	}
	return key, true
}

func nsForLog(ns string) string {
	if ns == "" {
		return "(all)"
	}
	return ns
}

// podContainerKeyMatchesPod reports whether containerKey
// ("namespace/name/container") belongs to podKey ("namespace/name").
func podContainerKeyMatchesPod(containerKey, podKey string) bool {
	return strings.HasPrefix(containerKey, podKey+"/")
}
