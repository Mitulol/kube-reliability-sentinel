// Command sentinel watches a Kubernetes cluster for Pod and Node
// reliability problems and reports them as structured logs, Kubernetes
// Events, and Prometheus metrics.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Mitulol/kube-reliability-sentinel/internal/alert"
	"github.com/Mitulol/kube-reliability-sentinel/internal/metrics"
	"github.com/Mitulol/kube-reliability-sentinel/internal/reliability"
	"github.com/Mitulol/kube-reliability-sentinel/internal/watcher"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
)

// Set via -ldflags at build time.
var (
	version = "dev"
	commit  = "none"
)

type flags struct {
	kubeconfig       string
	namespace        string
	metricsAddr      string
	allNamespaces    bool
	watchNodes       bool
	restartThreshold int
	pendingGrace     time.Duration
	nodeGrace        time.Duration
	rapidWindow      time.Duration
	rapidCount       int
	resendInterval   time.Duration
	clearGrace       time.Duration
	resyncPeriod     time.Duration
	workers          int
	enableEvents     bool
	logFormat        string
	showVersion      bool
}

// registerFlags binds every flag onto fs and returns the struct they write
// into. Split out from parseFlags so tests can drive it with their own
// FlagSet and argument slice instead of the process-global flag.CommandLine.
func registerFlags(fs *flag.FlagSet) *flags {
	f := &flags{}
	fs.StringVar(&f.kubeconfig, "kubeconfig", envOr("KUBECONFIG", ""), "path to kubeconfig; empty means in-cluster config, then ~/.kube/config")
	fs.StringVar(&f.namespace, "namespace", "", "namespace to watch; empty with --all-namespaces=false watches the namespace from kubeconfig context")
	fs.BoolVar(&f.allNamespaces, "all-namespaces", true, "watch Pods in every namespace")
	fs.BoolVar(&f.watchNodes, "watch-nodes", true, "also watch Nodes for NotReady / resource pressure")
	fs.StringVar(&f.metricsAddr, "metrics-addr", ":8080", "address for the /metrics, /healthz and /readyz server")
	fs.IntVar(&f.restartThreshold, "restart-threshold", 5, "per-container restart count that trips HighRestartCount when not Ready")
	fs.DurationVar(&f.pendingGrace, "pending-grace", 5*time.Minute, "how long a Pod may stay Pending before PodStuckPending fires")
	fs.DurationVar(&f.nodeGrace, "node-notready-grace", 2*time.Minute, "how long a Node may be NotReady before NodeNotReady fires")
	fs.DurationVar(&f.rapidWindow, "rapid-restart-window", 2*time.Minute, "sliding window for the rapid-restart burst detector")
	fs.IntVar(&f.rapidCount, "rapid-restart-count", 3, "restart-count increases within the window that trip RapidRestart (0 disables)")
	fs.DurationVar(&f.resendInterval, "resend-interval", 10*time.Minute, "minimum gap before an identical alert is delivered again")
	fs.DurationVar(&f.clearGrace, "clear-grace", 0, "how long an alert must be absent before it is considered resolved (0 = 2x resync, min 90s)")
	fs.DurationVar(&f.resyncPeriod, "resync-period", 60*time.Second, "informer resync period; also how often time-based rules are re-checked")
	fs.IntVar(&f.workers, "workers", 2, "number of reconcile workers")
	fs.BoolVar(&f.enableEvents, "enable-events", true, "emit Kubernetes Events for alerts (needs create permission on events)")
	fs.StringVar(&f.logFormat, "log-format", "json", "log format: json or text")
	fs.BoolVar(&f.showVersion, "version", false, "print version and exit")
	return f
}

func parseFlags() flags {
	f := registerFlags(flag.CommandLine)
	flag.Parse()
	return *f
}

func main() {
	f := parseFlags()
	if f.showVersion {
		fmt.Printf("kube-reliability-sentinel %s (%s)\n", version, commit)
		return
	}

	log := newLogger(f.logFormat)
	slog.SetDefault(log)

	if err := run(f, log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(f flags, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	restCfg, err := buildRESTConfig(f.kubeconfig)
	if err != nil {
		return fmt.Errorf("building REST config: %w", err)
	}
	restCfg.UserAgent = "kube-reliability-sentinel/" + version

	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("building clientset: %w", err)
	}

	m := metrics.New(version, commit)
	go func() {
		if err := m.Serve(ctx, f.metricsAddr); err != nil {
			log.Error("metrics server stopped", "err", err)
		}
	}()
	log.Info("metrics server listening", "addr", f.metricsAddr)

	// Build the alert pipeline: sinks -> deduper -> controller.
	sinks := alert.MultiSink{alert.NewSlogSink(log)}
	var broadcaster record.EventBroadcaster
	if f.enableEvents {
		broadcaster = record.NewBroadcaster()
		broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: client.CoreV1().Events("")})
		broadcaster.StartStructuredLogging(0)
		recorder := broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "kube-reliability-sentinel"})
		sinks = append(sinks, alert.NewEventSink(recorder, m))
	}
	deduper := alert.NewDeduper(sinks, f.resendInterval, m)

	namespace := ""
	if !f.allNamespaces {
		namespace = f.namespace
		if namespace == "" {
			namespace = namespaceFromKubeconfig(f.kubeconfig)
		}
	}

	th := reliability.Thresholds{
		RestartCountWarn:        int32(f.restartThreshold),
		PendingGracePeriod:      f.pendingGrace,
		NodeNotReadyGracePeriod: f.nodeGrace,
	}

	ctrl := watcher.New(client, watcher.Options{
		Namespace:          namespace,
		Thresholds:         th,
		WatchNodes:         f.watchNodes,
		ResyncPeriod:       f.resyncPeriod,
		RapidRestartWindow: f.rapidWindow,
		RapidRestartCount:  f.rapidCount,
		ClearGracePeriod:   f.clearGrace,
		Workers:            f.workers,
	}, deduper, deduper, m, log)

	log.Info("kube-reliability-sentinel starting",
		"version", version, "namespace", nsLabel(namespace),
		"watchNodes", f.watchNodes, "events", f.enableEvents)

	err = ctrl.Run(ctx)
	if broadcaster != nil {
		broadcaster.Shutdown()
	}
	return err
}

func buildRESTConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" {
		if cfg, err := rest.InClusterConfig(); err == nil {
			return cfg, nil
		}
	}
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		loadingRules.ExplicitPath = kubeconfig
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules, &clientcmd.ConfigOverrides{}).ClientConfig()
}

func namespaceFromKubeconfig(kubeconfig string) string {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		loadingRules.ExplicitPath = kubeconfig
	}
	ns, _, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules, &clientcmd.ConfigOverrides{}).Namespace()
	if err != nil || ns == "" {
		return "default"
	}
	return ns
}

func newLogger(format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if format == "text" {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func nsLabel(ns string) string {
	if ns == "" {
		return "(all)"
	}
	return ns
}
