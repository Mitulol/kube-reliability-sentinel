// Package metrics owns Sentinel's Prometheus surface and the HTTP server
// that exposes it (plus liveness/readiness probes).
//
// The metric set is deliberately small and answers the questions an on-call
// engineer asks first: is Sentinel actually watching the cluster, how many
// objects are unhealthy right now, and what has it alerted on.
package metrics

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/util/workqueue"
)

// Metrics is the registered metric set. Construct with New and pass it down
// to the controller and alert sink.
type Metrics struct {
	reg *prometheus.Registry

	PodsWatched   prometheus.Gauge
	NodesWatched  prometheus.Gauge
	UnhealthyPods prometheus.Gauge

	AlertsFired      *prometheus.CounterVec // labels: reason, severity
	AlertsSuppressed *prometheus.CounterVec // labels: reason
	EventsEmitted    *prometheus.CounterVec // labels: reason

	Reconciles       *prometheus.CounterVec   // labels: kind
	ReconcileErrors  *prometheus.CounterVec   // labels: kind
	ReconcileLatency *prometheus.HistogramVec // labels: kind

	InformerReconnects prometheus.Counter

	ready atomic.Bool
}

// New builds and registers the metric set. version/commit populate a
// build-info gauge.
func New(version, commit string) *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg: reg,
		PodsWatched: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sentinel_pods_watched",
			Help: "Number of Pods currently in Sentinel's informer cache.",
		}),
		NodesWatched: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sentinel_nodes_watched",
			Help: "Number of Nodes currently in Sentinel's informer cache.",
		}),
		UnhealthyPods: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sentinel_unhealthy_pods",
			Help: "Number of distinct Pods with at least one active alert at last reconcile.",
		}),
		AlertsFired: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sentinel_alerts_fired_total",
			Help: "Alerts delivered to the sink, by reason and severity.",
		}, []string{"reason", "severity"}),
		AlertsSuppressed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sentinel_alerts_suppressed_total",
			Help: "Alerts dropped by the deduper because an identical alert fired within the resend interval.",
		}, []string{"reason"}),
		EventsEmitted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sentinel_kube_events_emitted_total",
			Help: "Kubernetes Events written by the EventRecorder sink, by reason.",
		}, []string{"reason"}),
		Reconciles: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sentinel_reconcile_total",
			Help: "Work-queue items processed, by object kind.",
		}, []string{"kind"}),
		ReconcileErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sentinel_reconcile_errors_total",
			Help: "Work-queue items that returned an error and were requeued, by object kind.",
		}, []string{"kind"}),
		ReconcileLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "sentinel_reconcile_duration_seconds",
			Help:    "Wall time to process one work-queue item.",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5},
		}, []string{"kind"}),
		InformerReconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "sentinel_informer_reconnects_total",
			Help: "Times a shared informer watch had to be re-established (watch errors).",
		}),
	}

	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sentinel_build_info",
		Help: "Build metadata; value is always 1.",
	}, []string{"version", "commit", "goversion"})
	buildInfo.WithLabelValues(version, commit, goVersion()).Set(1)

	reg.MustRegister(
		m.PodsWatched, m.NodesWatched, m.UnhealthyPods,
		m.AlertsFired, m.AlertsSuppressed, m.EventsEmitted,
		m.Reconciles, m.ReconcileErrors, m.ReconcileLatency,
		m.InformerReconnects, buildInfo,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	// Route client-go's own work-queue metrics (depth, adds, latency,
	// retries) into the same registry so they are scraped alongside ours.
	workqueue.SetProvider(newWorkqueueProvider(reg))

	return m
}

// ObserveReconcile records one completed work-queue item.
func (m *Metrics) ObserveReconcile(kind string, d time.Duration, err error) {
	m.Reconciles.WithLabelValues(kind).Inc()
	m.ReconcileLatency.WithLabelValues(kind).Observe(d.Seconds())
	if err != nil {
		m.ReconcileErrors.WithLabelValues(kind).Inc()
	}
}

// AlertFired implements alert.Counters.
func (m *Metrics) AlertFired(reason, severity string) {
	m.AlertsFired.WithLabelValues(reason, severity).Inc()
}

// AlertSuppressed implements alert.Counters.
func (m *Metrics) AlertSuppressed(reason string) {
	m.AlertsSuppressed.WithLabelValues(reason).Inc()
}

// KubeEventEmitted implements alert.Counters.
func (m *Metrics) KubeEventEmitted(reason string) {
	m.EventsEmitted.WithLabelValues(reason).Inc()
}

// SetReady flips the readiness probe once the informer caches have synced.
func (m *Metrics) SetReady(v bool) { m.ready.Store(v) }

// Handler returns the HTTP handler serving /metrics, /healthz and /readyz.
func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{Registry: m.reg}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !m.ready.Load() {
			http.Error(w, "informer caches not synced", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	return mux
}

// Serve runs the metrics/probes HTTP server until ctx is cancelled.
func (m *Metrics) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           m.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// Registry exposes the underlying registry for tests.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }
