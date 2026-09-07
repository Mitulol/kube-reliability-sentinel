package k8sclient

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestListAndWatch exercises the list-then-watch loop end to end against a
// fake API server: an initial LIST, a streamed MODIFIED event, then a 410
// Gone on the watch that must force a relist. No Kubernetes required.
func TestListAndWatch(t *testing.T) {
	var listCalls, watchCalls int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isWatch := r.URL.Query().Get("watch") == "true"
		switch {
		case !isWatch:
			listCalls++
			fmt.Fprintf(w, `{"metadata":{"resourceVersion":"100"},"items":[
				{"metadata":{"name":"a","namespace":"default","resourceVersion":"90"},"status":{"phase":"Running"}}
			]}`)
		default:
			watchCalls++
			flusher := w.(http.Flusher)
			if watchCalls == 1 {
				// One real event, then simulate the watch expiring.
				fmt.Fprint(w, `{"type":"MODIFIED","object":{"metadata":{"name":"a","namespace":"default"},"status":{"phase":"Running","containerStatuses":[{"name":"app","restartCount":3,"state":{"waiting":{"reason":"CrashLoopBackOff"}}}]}}}`+"\n")
				flusher.Flush()
				fmt.Fprint(w, `{"type":"ERROR","object":{"code":410,"reason":"Expired","message":"too old resource version"}}`+"\n")
				flusher.Flush()
				return
			}
			// Second watch: block briefly so the test can cancel.
			fmt.Fprint(w, `{"type":"BOOKMARK","object":{"metadata":{"resourceVersion":"200"}}}`+"\n")
			flusher.Flush()
			<-r.Context().Done()
		}
	}))
	defer srv.Close()

	c, err := New(Config{Host: srv.URL, Namespace: "default", Insecure: true})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	type obs struct {
		typ WatchEventType
		pod string
	}
	seen := make(chan obs, 16)
	go func() {
		_ = c.ListAndWatch(ctx, func(et WatchEventType, p *Pod) {
			seen <- obs{et, p.Metadata.Name}
		})
	}()

	// Initial LIST delivers "a" as ADDED.
	if got := <-seen; got.typ != EventAdded || got.pod != "a" {
		t.Fatalf("first observation = %+v, want ADDED a", got)
	}
	// Streamed MODIFIED event.
	if got := <-seen; got.typ != EventModified || got.pod != "a" {
		t.Fatalf("second observation = %+v, want MODIFIED a", got)
	}
	// The 410 must have triggered a relist: "a" re-delivered as ADDED.
	if got := <-seen; got.typ != EventAdded {
		t.Fatalf("third observation = %+v, want ADDED after relist", got)
	}

	cancel()
	if listCalls < 2 {
		t.Fatalf("expected at least 2 LIST calls (initial + relist after 410), got %d", listCalls)
	}
}

func TestAPIErrorGone(t *testing.T) {
	e := &APIError{StatusCode: http.StatusGone, Body: "expired"}
	if !e.Gone() {
		t.Fatal("410 should report Gone")
	}
	if !strings.Contains(e.Error(), "410") {
		t.Fatalf("error string missing status: %q", e.Error())
	}
}

func TestWatchEventDecode(t *testing.T) {
	evt := WatchEvent{Type: EventModified}
	evt.Object = []byte(`{"metadata":{"name":"p","namespace":"ns"},"status":{"phase":"Running"}}`)
	pod, err := evt.AsPod()
	if err != nil {
		t.Fatal(err)
	}
	if pod.Key() != "ns/p" {
		t.Fatalf("key = %q, want ns/p", pod.Key())
	}
}
