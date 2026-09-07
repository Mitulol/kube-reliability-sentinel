# kube-reliability-sentinel

A small Kubernetes controller, in Go, that watches Pods and Nodes and reports
reliability problems the moment they appear: CrashLoopBackOff, OOMKilled
containers, rapid restart bursts, high restart counts, Pods stuck Pending
past a threshold, image-pull failures, and NotReady / resource-pressured
Nodes. It reports them three ways at once:

- **structured logs** (`slog`, JSON or text),
- **real Kubernetes Events** on the offending object, visible in
  `kubectl describe pod` / `kubectl get events` exactly like a first-party
  controller's, and
- **Prometheus metrics** at `/metrics` (alerts by reason/severity, unhealthy
  pod count, reconcile latency, work-queue depth, informer reconnects).

It is built to run as a Deployment inside the cluster it watches, with
least-privilege RBAC (`get/list/watch` on pods and nodes, `create/patch` on
events, nothing else).

```
                    ┌── SharedInformer (Pods)  ─┐
  API server ─watch─┤                            ├─► workqueue ─► reconcile ──► rule engine ──► deduper ──┬─► slog sink
                    └── SharedInformer (Nodes) ─┘   (rate-limited,               (pure fns,   (resend    ├─► Event sink (kubectl)
                                                     deduped, retried)            per object)  window)    └─► Prometheus /metrics
```

## Why it is shaped like a controller

Handling informer events inline is tempting and wrong. Sentinel uses the
standard client-go controller pattern: informers feed a
`workqueue.TypedRateLimitingInterface`, a fixed worker pool drains it, and
failed reconciles are re-queued with exponential backoff. That buys
coalescing of rapid updates to the same object, bounded concurrency, and
retry-with-backoff on transient errors, none of which you get from an inline
handler. The informer's resync period (default 60s) doubles as the trigger
for time-based rules (`PodStuckPending`, `NodeNotReady`) that no object
change would otherwise wake up.

The rule engine itself (`internal/reliability`) is deliberately pure: every
rule is a function from one object plus thresholds plus a reference time to
`[]Alert`, with no cluster access and no state. That is what makes it
exhaustively table-testable without an API server. Stateful detection that
genuinely needs history across observations, the rapid-restart burst
detector, lives in the controller (`internal/watcher/restart_tracker.go`),
not in the engine.

There is also a dependency-free, standard-library implementation of the
list-then-watch protocol in `internal/k8sclient`, with an end-to-end test
against a fake API server. It is **not** on the production path (client-go's
informers are); it is kept as an executable design note. See
[docs/watch-protocol.md](docs/watch-protocol.md).

## Rules

| Reason | Severity | Fires when |
| --- | --- | --- |
| `CrashLoopBackOff` | Critical | a container's `State.Waiting.Reason == CrashLoopBackOff` |
| `OOMKilled` | Critical | current or last termination `Reason == OOMKilled` (survives the restart) |
| `RapidRestart` | Critical | ≥ N restart-count increases observed within a sliding window (default 3 / 2m) |
| `HighRestartCount` | Warning | restart count ≥ threshold (default 5) **and** the container is not Ready |
| `ImagePullError` | Warning | `State.Waiting.Reason` in `ImagePullBackOff` / `ErrImagePull` |
| `PodStuckPending` | Warning | phase `Pending` for ≥ grace period (default 5m), measured from creation; suppressed if a container already has a specific waiting reason |
| `NodeNotReady` | Critical | `Ready` condition ≠ `True` for ≥ grace period (default 2m) |
| `NodePressure` | Warning | `MemoryPressure` / `DiskPressure` / `PIDPressure` condition is `True` |

Edge cases the tests pin down: a pod restarting but below threshold does not
alert; a container that crashed many times but is Ready again does not alert;
a recovered pod is cleared from the active set so a recurrence reports
immediately instead of being swallowed by the resend window; a pod being
deleted is never alerted on.

## Alert delivery

Alerts flow through a **deduper** before any sink: an identical alert (same
reason + object + container) is delivered at most once per `--resend-interval`
(default 10m), so a pod that sits in CrashLoopBackOff for an hour produces
one alert and one Event, not one per resync. Suppressed alerts are counted
(`sentinel_alerts_suppressed_total`). When the underlying problem clears, the
controller tells the deduper to forget that key.

## Quick start (local, kind)

```bash
make kind-up            # kind create cluster
make deploy             # docker build, kind load, kubectl apply -k manifests/base
make chaos              # apply hack/chaos-workloads.yaml (4 broken pods)
./hack/e2e-assert.sh    # assert Sentinel detected each failure mode
kubectl -n sentinel logs deploy/kube-reliability-sentinel -f
```

Out of cluster (against whatever your kubeconfig points at):

```bash
make build
./bin/sentinel --kubeconfig ~/.kube/config --all-namespaces --metrics-addr :8080
```

Flags: `--kubeconfig`, `--namespace` / `--all-namespaces`, `--watch-nodes`,
`--metrics-addr`, `--restart-threshold`, `--pending-grace`,
`--node-notready-grace`, `--rapid-restart-window`, `--rapid-restart-count`,
`--resend-interval`, `--resync-period`, `--workers`, `--enable-events`,
`--log-format`.

## Deployment & RBAC

`manifests/base` is a Kustomize base: `Namespace` (PodSecurity `restricted`),
`ServiceAccount`, a `ClusterRole` scoped to exactly `get/list/watch` on
`pods`/`nodes` and `create/patch` on `events`, `ClusterRoleBinding`, a
`Deployment` (distroless nonroot, read-only rootfs, all capabilities
dropped, CPU/memory limits, liveness on `/healthz`, readiness on
`/readyz`), and a `Service` for `/metrics`. `manifests/overlays/e2e` is a
thin overlay that only shortens the rule grace periods so CI does not wait
5 real minutes for `PodStuckPending`.

`manifests/gitops/` holds a Flux `GitRepository` + `Kustomization` and an
Argo CD `Application` pointed at `manifests/base`. **These are
illustrative:** they render clean but the validation below deployed with
`kubectl apply -k`, not a live GitOps pipeline. See
`manifests/gitops/README.md`.

## Real-cluster validation

<!-- VALIDATION:BEGIN -->
Run against a real `kind` cluster (Kubernetes **v1.37.0**, containerd
2.3.4), Sentinel deployed in-cluster from `manifests/base` via
`kubectl apply -k`. Then `hack/chaos-workloads.yaml` applied: four Pods,
each reproducing one failure mode, plus a node stopped on purpose.
`hack/e2e-assert.sh` automates the Pod checks; the numbers below are from
one such run.

**Detection latency** (wall time from `kubectl apply` of the broken Pod to
Sentinel delivering the alert):

| Failure mode | Workload | Detected reason | Latency |
| --- | --- | --- | --- |
| Image tag does not exist | `bad-image` | `ImagePullError` | **~1.6 s** |
| 250 MB alloc under a 32 MB limit | `memory-hog` | `OOMKilled` | **~1.6 s** |
| `exit 1` on a 2 s loop | `crasher` | `CrashLoopBackOff` | **~21 s** ¹ |
| 3 restarts inside 2 m | `memory-hog` | `RapidRestart` | ~38 s ² |
| 5 restarts, not Ready | `memory-hog` | `HighRestartCount` | ~85 s ² |
| `requests.cpu: 512` (unschedulable) | `unschedulable` | `PodStuckPending` | 5 m 0 s ³ |
| `docker stop` the worker node | `sentinel-dev-worker` | `NodeNotReady` | ~3 m 30 s ⁴ |

¹ Bounded by the kubelet: `State.Waiting.Reason` only becomes
`CrashLoopBackOff` once the kubelet's own restart backoff engages (~10 s
after the second crash). Sentinel reacts to that transition in < 1 reconcile.
² Bounded by how fast the workload actually accumulates restarts.
³ Exactly the configured `--pending-grace`; the alert message is pulled
from the Pod's `PodScheduled` condition: _"0/1 nodes are available: 1
Insufficient cpu."_
⁴ The node's `Ready` condition flipped to `Unknown` ~47 s after the stop
(kube-controller-manager's node-monitor grace), then Sentinel's own
`--node-notready-grace` (2 m) plus up to one 60 s resync: alert message
_"Node Ready=Unknown for 3m0s (>= 2m0s): Kubelet stopped posting node
status."_ Sentinel ran pinned to the control-plane node so stopping the
worker did not take it down with it.

**Sentinel's own overhead is not the bottleneck.** From the in-cluster
`/metrics` scrape after the run (176 Pod reconciles, 10 Node reconciles):

- `sentinel_reconcile_duration_seconds`: sum 4.9 ms over 176 samples →
  **mean ≈ 28 µs per reconcile**, all samples < 500 µs.
- `sentinel_workqueue_queue_duration_seconds`: mean ≈ 6 ms, 94 % < 1 ms
  (the tail is items delayed by the rate limiter during the restart storm).
- Deployed resource envelope: `requests 25m / 64Mi`, `limits 200m / 128Mi`;
  actual steady-state well under requests on a 14-Pod cluster.

**Deduplication held.** Each reason produced exactly one alert and one
Kubernetes Event per object across the run, while the deduper suppressed the
repeats it would otherwise have sent:

```
sentinel_alerts_fired_total{reason="OOMKilled"}          1
sentinel_alerts_suppressed_total{reason="OOMKilled"}     25
sentinel_alerts_fired_total{reason="CrashLoopBackOff"}   2     # one per Pod
sentinel_alerts_suppressed_total{reason="CrashLoopBackOff"} 14
sentinel_kube_events_emitted_total{reason="OOMKilled"}   1
sentinel_pods_watched      14
sentinel_unhealthy_pods    4
```

**The Events are real.** `kubectl describe pod bad-image` shows Sentinel's
finding inline with the kubelet's own, attributed and field-path-scoped:

```
Warning  SentinelImagePullError  2m47s  kube-reliability-sentinel  spec.containers{app}: Container "app" cannot pull its image (ErrImagePull): ...
```

**Not validated live:** `NodePressure` (inducing real memory/disk/PID
pressure on a kind node reliably is fiddly). It is covered by a unit test
and shares the exact condition-reading shape as `NodeNotReady`, which was
validated live above. The GitOps manifests in `manifests/gitops/` are
illustrative, not operated (see `manifests/gitops/README.md`).
<!-- VALIDATION:END -->

## Testing

```bash
make race     # go test ./... -race
make cover
```

- `internal/reliability` — table-driven tests for every rule against
  constructed Pod/Node fixtures, including the recovery / below-threshold
  edge cases.
- `internal/watcher` — controller tests on `k8s.io/client-go/kubernetes/fake`:
  a fake clientset, real informers, Add/Update events pushed through, alert
  delivery and active-set clearing asserted. Plus unit tests for the restart
  burst detector (window expiry, stable-count suppression, per-pod forget).
- `internal/alert` — deduper suppression / resend / forget, fan-out, the
  slog sink's structured output, the Event sink against a `FakeRecorder`.
- `internal/metrics` — `/metrics`, `/healthz`, `/readyz` behavior and
  counter wiring.
- `internal/k8sclient` — the list-then-watch loop end to end against an
  `httptest` server, including a forced 410 Gone → relist.

Everything runs under `-race`.

## Layout

```
cmd/sentinel/          flags, wiring, signal handling
internal/reliability/  pure rule engine + Alert type
internal/watcher/      informer + workqueue controller, restart burst tracker
internal/alert/        deduper, slog sink, Kubernetes Event sink
internal/metrics/      Prometheus registry + HTTP server, workqueue metrics adapter
internal/k8sclient/    dependency-free list-then-watch (design note, not prod path)
manifests/base/        Kustomize base: RBAC, Deployment, Service
manifests/overlays/    e2e overlay (short grace periods for CI)
manifests/gitops/      illustrative Flux / Argo CD wiring
hack/                  chaos workloads, e2e assertion script
docs/                  watch-protocol design note
```

## License

MIT
