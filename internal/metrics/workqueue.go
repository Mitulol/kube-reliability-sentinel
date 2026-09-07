package metrics

import (
	"errors"
	"runtime/debug"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/util/workqueue"
)

// workqueueProvider adapts client-go's workqueue.MetricsProvider onto a
// Prometheus registry, so the standard controller work-queue signals
// (queue depth, add rate, queue-wait latency, work duration, retry count)
// are scraped from the same /metrics endpoint as Sentinel's own metrics.
type workqueueProvider struct{ reg prometheus.Registerer }

func newWorkqueueProvider(reg prometheus.Registerer) workqueue.MetricsProvider {
	return &workqueueProvider{reg: reg}
}

// register adds c to the registry, tolerating a duplicate registration by
// returning the already-registered collector instead. client-go installs
// the metrics provider process-wide via a sync.Once, so in a test binary
// that builds several registries the provider stays bound to the first one
// and a second queue of the same name would otherwise panic.
func register[T prometheus.Collector](reg prometheus.Registerer, c T) T {
	if err := reg.Register(c); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(T); ok {
				return existing
			}
		}
	}
	return c
}

func (p *workqueueProvider) gauge(name, help, queue string) prometheus.Gauge {
	return register(p.reg, prometheus.NewGauge(prometheus.GaugeOpts{
		Subsystem: "sentinel_workqueue", Name: name, Help: help,
		ConstLabels: prometheus.Labels{"queue": queue},
	}))
}

func (p *workqueueProvider) counter(name, help, queue string) prometheus.Counter {
	return register(p.reg, prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: "sentinel_workqueue", Name: name, Help: help,
		ConstLabels: prometheus.Labels{"queue": queue},
	}))
}

func (p *workqueueProvider) histogram(name, help, queue string) prometheus.Histogram {
	return register(p.reg, prometheus.NewHistogram(prometheus.HistogramOpts{
		Subsystem: "sentinel_workqueue", Name: name, Help: help,
		ConstLabels: prometheus.Labels{"queue": queue},
		Buckets:     []float64{0.001, 0.01, 0.1, 0.5, 1, 5, 30, 60},
	}))
}

func (p *workqueueProvider) NewDepthMetric(name string) workqueue.GaugeMetric {
	return p.gauge("depth", "Current depth of the work queue.", name)
}
func (p *workqueueProvider) NewAddsMetric(name string) workqueue.CounterMetric {
	return p.counter("adds_total", "Total adds to the work queue.", name)
}
func (p *workqueueProvider) NewLatencyMetric(name string) workqueue.HistogramMetric {
	return p.histogram("queue_duration_seconds", "Time an item waits in the queue before processing.", name)
}
func (p *workqueueProvider) NewWorkDurationMetric(name string) workqueue.HistogramMetric {
	return p.histogram("work_duration_seconds", "Time to process an item from the queue.", name)
}
func (p *workqueueProvider) NewUnfinishedWorkSecondsMetric(name string) workqueue.SettableGaugeMetric {
	return p.gauge("unfinished_work_seconds", "Seconds of work in progress that has not been observed as done.", name)
}
func (p *workqueueProvider) NewLongestRunningProcessorSecondsMetric(name string) workqueue.SettableGaugeMetric {
	return p.gauge("longest_running_processor_seconds", "Age of the longest-running in-flight item.", name)
}
func (p *workqueueProvider) NewRetriesMetric(name string) workqueue.CounterMetric {
	return p.counter("retries_total", "Total retries (rate-limited re-adds) for the queue.", name)
}

func goVersion() string {
	if bi, ok := debug.ReadBuildInfo(); ok {
		return bi.GoVersion
	}
	return "unknown"
}
