package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRegisterFlagsDefaults(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	f := registerFlags(fs)
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	if !f.allNamespaces || !f.watchNodes || !f.enableEvents {
		t.Errorf("expected all-namespaces/watch-nodes/enable-events to default true, got %+v", f)
	}
	if f.pendingGrace != 5*time.Minute || f.nodeGrace != 2*time.Minute {
		t.Errorf("grace defaults wrong: pending=%s node=%s", f.pendingGrace, f.nodeGrace)
	}
	if f.restartThreshold != 5 || f.rapidCount != 3 {
		t.Errorf("threshold defaults wrong: restart=%d rapid=%d", f.restartThreshold, f.rapidCount)
	}
	if f.metricsAddr != ":8080" || f.logFormat != "json" {
		t.Errorf("addr/format defaults wrong: %q %q", f.metricsAddr, f.logFormat)
	}
}

func TestRegisterFlagsOverrides(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	f := registerFlags(fs)
	args := []string{
		"--all-namespaces=false", "--namespace=prod", "--watch-nodes=false",
		"--restart-threshold=10", "--pending-grace=90s", "--metrics-addr=:9000",
		"--log-format=text", "--rapid-restart-count=0",
	}
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	if f.allNamespaces || f.watchNodes {
		t.Error("expected all-namespaces and watch-nodes to be false")
	}
	if f.namespace != "prod" || f.restartThreshold != 10 || f.pendingGrace != 90*time.Second {
		t.Errorf("override not applied: %+v", f)
	}
	if f.metricsAddr != ":9000" || f.logFormat != "text" || f.rapidCount != 0 {
		t.Errorf("override not applied: %+v", f)
	}
}

func TestEnvOr(t *testing.T) {
	const key = "SENTINEL_TEST_ENVOR"
	t.Setenv(key, "")
	if got := envOr(key, "fallback"); got != "fallback" {
		t.Errorf("empty env should yield default, got %q", got)
	}
	t.Setenv(key, "set-value")
	if got := envOr(key, "fallback"); got != "set-value" {
		t.Errorf("set env should win, got %q", got)
	}
}

func TestNsLabel(t *testing.T) {
	if nsLabel("") != "(all)" {
		t.Error(`empty namespace should render as "(all)"`)
	}
	if nsLabel("kube-system") != "kube-system" {
		t.Error("non-empty namespace should render verbatim")
	}
}

func TestNewLogger(t *testing.T) {
	if newLogger("text") == nil || newLogger("json") == nil || newLogger("") == nil {
		t.Fatal("newLogger returned nil")
	}
	// "" and anything unrecognized fall through to JSON; just assert no panic
	// and a usable logger.
	newLogger("").Info("smoke")
}

const testKubeconfig = `
apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: https://127.0.0.1:6443
    insecure-skip-tls-verify: true
contexts:
- name: test
  context:
    cluster: test
    user: test
    namespace: team-a
current-context: test
users:
- name: test
  user:
    token: abc123
`

func writeKubeconfig(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, []byte(testKubeconfig), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestBuildRESTConfigFromKubeconfig(t *testing.T) {
	path := writeKubeconfig(t)
	cfg, err := buildRESTConfig(path)
	if err != nil {
		t.Fatalf("buildRESTConfig: %v", err)
	}
	if cfg.Host != "https://127.0.0.1:6443" {
		t.Errorf("host = %q, want https://127.0.0.1:6443", cfg.Host)
	}
	if cfg.BearerToken != "abc123" {
		t.Errorf("token = %q, want abc123", cfg.BearerToken)
	}
}

func TestNamespaceFromKubeconfig(t *testing.T) {
	path := writeKubeconfig(t)
	if ns := namespaceFromKubeconfig(path); ns != "team-a" {
		t.Errorf("namespace = %q, want team-a", ns)
	}
	// A missing file falls back to "default" rather than erroring out.
	if ns := namespaceFromKubeconfig(filepath.Join(t.TempDir(), "nope")); ns != "default" {
		t.Errorf("missing kubeconfig should fall back to default, got %q", ns)
	}
}
