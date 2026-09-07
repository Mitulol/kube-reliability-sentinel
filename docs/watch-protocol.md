# Design note: how the watch protocol works under the informer

Sentinel's production path uses `k8s.io/client-go`'s `SharedInformerFactory`,
because that is the library every real Kubernetes controller is built on and
it has years of edge-case handling that a hand-rolled client does not. But
the informer is not magic, and `internal/k8sclient` is a small,
dependency-free (standard library only) implementation of the same core loop,
kept so the mechanism is legible and testable on its own
(`internal/k8sclient/client_test.go` runs it end to end against an
`httptest` server, no cluster required).

## The list-then-watch loop

An informer keeps a local cache of objects in sync with the API server using
exactly two verbs:

1. **LIST** `/api/v1/pods` once. This seeds the cache and, crucially, returns
   a `metadata.resourceVersion` — a cluster-wide logical clock value that
   means "the state of the world as of here."

2. **WATCH** `/api/v1/pods?watch=true&resourceVersion=<from the list>`. This
   is a long-lived HTTP response that never ends on its own; the API server
   streams newline-delimited JSON objects, one per change:

   ```json
   {"type":"ADDED",   "object":{ ...Pod... }}
   {"type":"MODIFIED","object":{ ...Pod... }}
   {"type":"DELETED", "object":{ ...Pod... }}
   ```

   Each object carries a newer `resourceVersion`. The client tracks the
   highest one it has seen so it can resume from there.

## Why it is a loop, not a single watch

A watch connection does not live forever:

- **The API server closes it.** etcd only keeps a bounded history of
  changes (watch cache / compaction). Idle watches are also cut after a
  timeout. When the client reconnects with a `resourceVersion` that has
  been compacted away, the server replies with an `ERROR` event wrapping a
  `Status{code: 410, reason: "Expired"}`. The only recovery is to **relist**
  from scratch and start a fresh watch from the new resourceVersion. This is
  the single most important edge case, and the reason `List` and `Watch`
  cannot be separate one-shot calls.

- **The network breaks.** A dead TCP connection can hang silently. The
  client asks for `allowWatchBookmarks=true` so the server periodically
  sends a no-op `BOOKMARK` event (just a resourceVersion checkpoint), which
  both advances the resume point and proves the connection is alive.

- **On any other stream error**, back off briefly and relist.

`Client.ListAndWatch` in `internal/k8sclient/client.go` is that loop in ~40
lines. `SharedInformer` does the same thing, plus: a thread-safe indexed
store, resync timers, multiple event handlers per informer, shared watches
across informers of the same type, and paginated / chunked initial lists.

## What Sentinel adds on top

The informer only gets objects into a local cache and fires callbacks. The
standard controller pattern — which Sentinel follows — puts a **work queue**
between the callbacks and the actual work:

```
informer callbacks --Add(key)--> workqueue --Get()--> reconcile(key)
```

The queue gives three things that handling events inline in the callback
does not:

- **Coalescing:** ten rapid updates to one Pod between reconciles collapse
  to one queue entry, so the rule engine runs once, not ten times.
- **Rate-limited retry:** a reconcile that fails (e.g. a transient lister
  error) is re-added with exponential backoff via
  `workqueue.DefaultTypedControllerRateLimiter`, not retried in a hot loop.
- **Bounded concurrency:** a fixed worker pool drains the queue, instead of
  one goroutine per event.

The informer's *resync period* (Sentinel default: 60s) is also what makes
time-based rules work. `PodStuckPending` and `NodeNotReady` depend on wall
time, not on an object changing — a Pod that has been Pending for four
minutes looks identical to the API server as it crosses five. The resync
re-delivers every cached object through the update handler on a timer, so
the rule engine re-evaluates it and the threshold eventually trips.
