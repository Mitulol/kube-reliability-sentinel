// Package watcher is the controller: SharedInformer-fed work queue in,
// reliability alerts out.
//
// It follows the standard client-go controller shape deliberately, because
// that shape is the answer to "why not just alert straight from the
// informer event handler?": the work queue gives us dedup of rapid-fire
// updates to the same object, rate-limited exponential backoff on transient
// errors, and a fixed pool of workers instead of unbounded goroutines. The
// informer's own resync period doubles as the trigger for time-based rules
// (PodStuckPending, NodeNotReady) that no object change would otherwise
// wake up.
package watcher

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/Mitulol/kube-reliability-sentinel/internal/alert"
	"github.com/Mitulol/kube-reliability-sentinel/internal/metrics"
	"github.com/Mitulol/kube-reliability-sentinel/internal/reliability"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// Options configures a Controller.
type Options struct {
	// Namespace to watch Pods in; empty means all namespaces.
	Namespace string
	// Thresholds for the rule engine.
	Thresholds reliability.Thresholds
	// WatchNodes enables the cluster-scoped Node informer and Node rules.
	WatchNodes bool
	// ResyncPeriod is how often the informer re-delivers every cached
	// object, which is also how often time-based rules get re-checked.
	ResyncPeriod time.Duration
	// RapidRestartWindow / RapidRestartCount define the burst detector:
	// this many observed restart-count increases within this window fires
	// a RapidRestart alert.
	RapidRestartWindow time.Duration
	RapidRestartCount  int
	// ClearGracePeriod is how long an alert must be absent from an object's
	// reconcile results before it is considered resolved and its dedup
	// record is forgotten. Without this, a container that flaps between
	// Waiting=CrashLoopBackOff and Running (which it does on every restart)
	// would defeat the resend interval, re-alerting on every flap. Defaults
	// to 2x ResyncPeriod (min 90s) if zero.
	ClearGracePeriod time.Duration
	// Workers is the size of the reconcile worker pool.
	Workers int
}

// Controller wires an informer factory to the rule engine and alert sink.
type Controller struct {
	client kubernetes.Interface
	opts   Options
	log    *slog.Logger

	sink    alert.Sink
	deduper *alert.Deduper
	metrics *metrics.Metrics
	tracker *restartTracker

	podFactory  informers.SharedInformerFactory
	nodeFactory informers.SharedInformerFactory

	podLister   corelisters.PodLister
	nodeLister  corelisters.NodeLister
	podsSynced  cache.InformerSynced
	nodesSynced cache.InformerSynced

	queue workqueue.TypedRateLimitingInterface[queueKey]

	// active maps an object key ("Pod|ns/name") to the alert keys currently
	// tracked for it. An entry with a zero clearingSince is firing; a
	// non-zero one is in its clear grace period and will be forgotten (so a
	// recurrence reports immediately) once ClearGracePeriod elapses.
	mu     sync.Mutex
	active map[string]map[string]alertState

	clearGrace time.Duration
}

type alertState struct {
	clearingSince time.Time // zero == currently firing
}

type queueKey struct {
	kind      string // "Pod" or "Node"
	namespace string
	name      string
}

func (k queueKey) objectKey() string {
	if k.namespace == "" {
		return k.kind + "|" + k.name
	}
	return k.kind + "|" + k.namespace + "/" + k.name
}

// New builds a Controller. sink is the head of the alert pipeline (normally
// a *alert.Deduper wrapping the real sinks); deduper is that same deduper,
// passed separately so the controller can call Forget on it.
func New(client kubernetes.Interface, opts Options, sink alert.Sink, deduper *alert.Deduper, m *metrics.Metrics, log *slog.Logger) *Controller {
	if opts.Workers <= 0 {
		opts.Workers = 2
	}
	clearGrace := opts.ClearGracePeriod
	if clearGrace <= 0 {
		clearGrace = 2 * opts.ResyncPeriod
		if clearGrace < 90*time.Second {
			clearGrace = 90 * time.Second
		}
	}
	c := &Controller{
		client:  client,
		opts:    opts,
		log:     log,
		sink:    sink,
		deduper: deduper,
		metrics: m,
		tracker: newRestartTracker(opts.RapidRestartWindow, opts.RapidRestartCount),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[queueKey](),
			workqueue.TypedRateLimitingQueueConfig[queueKey]{Name: "sentinel"},
		),
		active:     make(map[string]map[string]alertState),
		clearGrace: clearGrace,
	}

	podOpts := []informers.SharedInformerOption{}
	if opts.Namespace != "" {
		podOpts = append(podOpts, informers.WithNamespace(opts.Namespace))
	}
	c.podFactory = informers.NewSharedInformerFactoryWithOptions(client, opts.ResyncPeriod, podOpts...)
	podInformer := c.podFactory.Core().V1().Pods()
	c.podLister = podInformer.Lister()
	c.podsSynced = podInformer.Informer().HasSynced
	_, _ = podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { c.enqueuePod(obj) },
		UpdateFunc: func(_, obj any) { c.enqueuePod(obj) },
		DeleteFunc: func(obj any) { c.enqueuePod(obj) },
	})
	_ = podInformer.Informer().SetWatchErrorHandler(func(_ *cache.Reflector, err error) {
		c.metrics.InformerReconnects.Inc()
		c.log.Warn("pod informer watch error, will re-establish", "err", err)
	})

	if opts.WatchNodes {
		// Nodes are cluster-scoped: they get their own, un-namespaced
		// factory so a --namespace flag never accidentally filters them.
		c.nodeFactory = informers.NewSharedInformerFactory(client, opts.ResyncPeriod)
		nodeInformer := c.nodeFactory.Core().V1().Nodes()
		c.nodeLister = nodeInformer.Lister()
		c.nodesSynced = nodeInformer.Informer().HasSynced
		_, _ = nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj any) { c.enqueueNode(obj) },
			UpdateFunc: func(_, obj any) { c.enqueueNode(obj) },
			DeleteFunc: func(obj any) { c.enqueueNode(obj) },
		})
		_ = nodeInformer.Informer().SetWatchErrorHandler(func(_ *cache.Reflector, err error) {
			c.metrics.InformerReconnects.Inc()
			c.log.Warn("node informer watch error, will re-establish", "err", err)
		})
	}

	return c
}

func (c *Controller) enqueuePod(obj any) {
	key, ok := metaNamespaceKey(obj)
	if !ok {
		return
	}
	ns, name, _ := cache.SplitMetaNamespaceKey(key)
	c.queue.Add(queueKey{kind: "Pod", namespace: ns, name: name})
}

func (c *Controller) enqueueNode(obj any) {
	key, ok := metaNamespaceKey(obj)
	if !ok {
		return
	}
	_, name, _ := cache.SplitMetaNamespaceKey(key)
	c.queue.Add(queueKey{kind: "Node", name: name})
}

// Run starts the informers and the worker pool and blocks until ctx is
// cancelled.
func (c *Controller) Run(ctx context.Context) error {
	defer c.queue.ShutDown()

	c.log.Info("starting informers",
		"namespace", nsForLog(c.opts.Namespace),
		"watchNodes", c.opts.WatchNodes,
		"resync", c.opts.ResyncPeriod)

	c.podFactory.Start(ctx.Done())
	syncers := []cache.InformerSynced{c.podsSynced}
	if c.opts.WatchNodes {
		c.nodeFactory.Start(ctx.Done())
		syncers = append(syncers, c.nodesSynced)
	}
	if !cache.WaitForCacheSync(ctx.Done(), syncers...) {
		return fmt.Errorf("watcher: informer caches failed to sync")
	}
	c.log.Info("informer caches synced")
	c.metrics.SetReady(true)

	var wg sync.WaitGroup
	for i := 0; i < c.opts.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c.processNext(ctx) {
			}
		}()
	}

	// Periodically refresh the "how many objects are we watching" gauges.
	go c.pollGauges(ctx)

	<-ctx.Done()
	c.log.Info("shutdown signal received, draining work queue")
	c.queue.ShutDown()
	wg.Wait()
	return nil
}

func (c *Controller) processNext(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	start := time.Now()
	err := c.reconcile(ctx, key)
	c.metrics.ObserveReconcile(key.kind, time.Since(start), err)

	if err != nil {
		if c.queue.NumRequeues(key) < 5 {
			c.log.Warn("reconcile failed, requeueing", "key", key.objectKey(), "err", err)
			c.queue.AddRateLimited(key)
			return true
		}
		c.log.Error("reconcile failed permanently, dropping", "key", key.objectKey(), "err", err)
	}
	c.queue.Forget(key)
	return true
}

func (c *Controller) reconcile(ctx context.Context, key queueKey) error {
	switch key.kind {
	case "Pod":
		return c.reconcilePod(ctx, key)
	case "Node":
		return c.reconcileNode(ctx, key)
	default:
		return fmt.Errorf("watcher: unknown kind %q", key.kind)
	}
}

func (c *Controller) reconcilePod(ctx context.Context, key queueKey) error {
	now := time.Now()
	pod, err := c.podLister.Pods(key.namespace).Get(key.name)
	if apierrors.IsNotFound(err) {
		c.clearObject(key.objectKey())
		c.tracker.forgetPod(key.namespace + "/" + key.name)
		return nil
	}
	if err != nil {
		return err
	}

	alerts := reliability.EvaluatePod(pod, c.opts.Thresholds, now)
	alerts = append(alerts, c.rapidRestartAlerts(pod, now)...)
	c.deliver(ctx, key.objectKey(), pod.Namespace, pod.Name, string(pod.UID), alerts)
	return nil
}

func (c *Controller) reconcileNode(ctx context.Context, key queueKey) error {
	now := time.Now()
	node, err := c.nodeLister.Get(key.name)
	if apierrors.IsNotFound(err) {
		c.clearObject(key.objectKey())
		return nil
	}
	if err != nil {
		return err
	}
	alerts := reliability.EvaluateNode(node, c.opts.Thresholds, now)
	c.deliver(ctx, key.objectKey(), "", node.Name, string(node.UID), alerts)
	return nil
}

// rapidRestartAlerts consults the stateful restart tracker and synthesizes
// a RapidRestart alert per container that is currently bursting.
func (c *Controller) rapidRestartAlerts(pod *corev1.Pod, now time.Time) []reliability.Alert {
	if c.opts.RapidRestartCount <= 0 {
		return nil
	}
	var out []reliability.Alert
	podKey := pod.Namespace + "/" + pod.Name
	for _, cs := range pod.Status.ContainerStatuses {
		ckey := podKey + "/" + cs.Name
		burst, n := c.tracker.observe(ckey, cs.RestartCount, now)
		if burst {
			out = append(out, reliability.Alert{
				Reason:    reliability.ReasonRapidRestart,
				Severity:  reliability.SeverityCritical,
				Kind:      "Pod",
				Namespace: pod.Namespace,
				Name:      pod.Name,
				UID:       string(pod.UID),
				Container: cs.Name,
				Message: fmt.Sprintf("Container %q restarted %d times within %s",
					cs.Name, n, c.opts.RapidRestartWindow),
			})
		}
	}
	return out
}

// deliver fires the current alert set for one object and ages out any alert
// that was firing before but is now absent: it enters a clear grace period,
// and only once that elapses is its dedup record forgotten (so a genuine
// recurrence reports immediately, while a flapping container does not
// defeat the resend interval).
func (c *Controller) deliver(ctx context.Context, objKey, ns, name, uid string, alerts []reliability.Alert) {
	now := time.Now()
	current := make(map[string]struct{}, len(alerts))
	for _, a := range alerts {
		current[a.Key()] = struct{}{}
		c.sink.Fire(ctx, a)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	states := c.active[objKey]
	if states == nil {
		states = make(map[string]alertState)
	}
	for k := range current {
		states[k] = alertState{} // firing; cancels any in-progress clear
	}
	for k, st := range states {
		if _, still := current[k]; still {
			continue
		}
		if st.clearingSince.IsZero() {
			states[k] = alertState{clearingSince: now}
		} else if now.Sub(st.clearingSince) >= c.clearGrace {
			c.deduper.Forget(k)
			delete(states, k)
		}
	}
	if len(states) == 0 {
		delete(c.active, objKey)
	} else {
		c.active[objKey] = states
	}
}

func (c *Controller) clearObject(objKey string) {
	c.mu.Lock()
	for k := range c.active[objKey] {
		c.deduper.Forget(k)
	}
	delete(c.active, objKey)
	c.mu.Unlock()
}

func (c *Controller) pollGauges(ctx context.Context) {
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			c.refreshGauges()
		}
	}
}

func (c *Controller) refreshGauges() {
	if pods, err := c.podLister.List(labels.Everything()); err == nil {
		c.metrics.PodsWatched.Set(float64(len(pods)))
	}
	if c.opts.WatchNodes && c.nodeLister != nil {
		if nodes, err := c.nodeLister.List(labels.Everything()); err == nil {
			c.metrics.NodesWatched.Set(float64(len(nodes)))
		}
	}
	c.mu.Lock()
	unhealthy := 0
	for k, states := range c.active {
		if !strings.HasPrefix(k, "Pod|") {
			continue
		}
		for _, st := range states {
			if st.clearingSince.IsZero() {
				unhealthy++
				break
			}
		}
	}
	c.mu.Unlock()
	c.metrics.UnhealthyPods.Set(float64(unhealthy))
}
