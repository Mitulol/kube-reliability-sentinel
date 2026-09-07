package k8sclient

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"
)

// Config describes how to reach a Kubernetes API server. It is populated
// either from in-cluster defaults (service account token + CA + namespace,
// the mode Sentinel actually runs in when deployed as a Pod) or explicitly
// via flags for out-of-cluster testing. Full kubeconfig (YAML) parsing is
// intentionally not implemented -- see README for why.
type Config struct {
	Host      string // e.g. "https://10.0.0.1:443"
	Token     string
	Namespace string
	CACert    []byte // PEM; nil means system trust store
	Insecure  bool   // skip TLS verification (testing only)
}

const (
	inClusterCACertPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	inClusterTokenPath     = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	inClusterNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
)

// InClusterConfig builds a Config from the standard service-account files
// Kubernetes mounts into every Pod, and the KUBERNETES_SERVICE_HOST/PORT
// env vars it injects. This is the mode Sentinel is meant to run in: as a
// Deployment inside the cluster it watches, same as any real controller.
func InClusterConfig() (*Config, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("k8sclient: not running in-cluster (KUBERNETES_SERVICE_HOST/PORT unset)")
	}
	token, err := os.ReadFile(inClusterTokenPath)
	if err != nil {
		return nil, fmt.Errorf("k8sclient: reading service account token: %w", err)
	}
	ca, err := os.ReadFile(inClusterCACertPath)
	if err != nil {
		return nil, fmt.Errorf("k8sclient: reading service account CA cert: %w", err)
	}
	ns, err := os.ReadFile(inClusterNamespacePath)
	if err != nil {
		return nil, fmt.Errorf("k8sclient: reading service account namespace: %w", err)
	}
	return &Config{
		Host:      "https://" + net_JoinHostPort(host, port),
		Token:     string(token),
		Namespace: string(ns),
		CACert:    ca,
	}, nil
}

// net_JoinHostPort avoids importing net just for this one call's brackets
// handling; kept trivially simple since host is always an IP here.
func net_JoinHostPort(host, port string) string {
	return host + ":" + port
}

// Client is a minimal REST client for the Pod List/Watch endpoints.
type Client struct {
	cfg        Config
	httpClient *http.Client
}

// New builds a Client from cfg, setting up TLS trust as configured.
func New(cfg Config) (*Client, error) {
	tlsConf := &tls.Config{InsecureSkipVerify: cfg.Insecure}
	if len(cfg.CACert) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(cfg.CACert) {
			return nil, fmt.Errorf("k8sclient: no certificates parsed from CACert")
		}
		tlsConf.RootCAs = pool
	}
	return &Client{
		cfg: cfg,
		httpClient: &http.Client{
			Transport: &http.Transport{TLSClientConfig: tlsConf},
			// No overall Timeout: the watch request is a long-lived stream.
			// Per-request deadlines are applied via context instead.
		},
	}, nil
}

func (c *Client) podsURL(watch bool, resourceVersion string) string {
	base, err := url.Parse(c.cfg.Host)
	if err != nil {
		// cfg.Host is validated at Config construction time in practice;
		// fall back to treating it as already-correct rather than panic.
		base = &url.URL{Scheme: "https", Host: c.cfg.Host}
	}
	u := &url.URL{
		Scheme: base.Scheme,
		Host:   base.Host,
		Path:   fmt.Sprintf("/api/v1/namespaces/%s/pods", c.cfg.Namespace),
	}
	q := u.Query()
	if watch {
		q.Set("watch", "true")
		if resourceVersion != "" {
			q.Set("resourceVersion", resourceVersion)
		}
		// Ask the API server for a periodic no-op keepalive event so a dead
		// TCP connection is noticed quickly rather than hanging silently.
		q.Set("timeoutSeconds", "300")
		q.Set("allowWatchBookmarks", "true")
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func (c *Client) newRequest(ctx context.Context, rawURL string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	if c.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	}
	req.Header.Set("Accept", "application/json")
	return req, nil
}

// List fetches the current set of Pods in cfg.Namespace and the
// resourceVersion a subsequent Watch should start from.
func (c *Client) List(ctx context.Context) (*PodList, error) {
	req, err := c.newRequest(ctx, c.podsURL(false, ""))
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("k8sclient: list request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, &APIError{StatusCode: resp.StatusCode, Body: string(body)}
	}
	var list PodList
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, fmt.Errorf("k8sclient: decoding list response: %w", err)
	}
	return &list, nil
}

// APIError is returned when the API server responds with a non-200 status.
// Gone (410) specifically means the requested resourceVersion has been
// compacted out of etcd's history and the caller must relist.
type APIError struct {
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("k8sclient: API server returned %d: %s", e.StatusCode, e.Body)
}

// Gone reports whether the error represents an HTTP 410, i.e. "your
// resourceVersion is too old, relist".
func (e *APIError) Gone() bool { return e.StatusCode == http.StatusGone }

// WatchStream is an open, streaming connection to the watch endpoint.
// Callers must call Close when done.
type WatchStream struct {
	body    io.ReadCloser
	scanner *bufio.Scanner
}

// Next blocks until the next watch event is available, ctx is cancelled, or
// the stream ends (io.EOF). The API server sends one JSON object per line.
func (w *WatchStream) Next() (WatchEvent, error) {
	if !w.scanner.Scan() {
		if err := w.scanner.Err(); err != nil {
			return WatchEvent{}, err
		}
		return WatchEvent{}, io.EOF
	}
	var evt WatchEvent
	if err := json.Unmarshal(w.scanner.Bytes(), &evt); err != nil {
		return WatchEvent{}, fmt.Errorf("k8sclient: decoding watch event: %w", err)
	}
	return evt, nil
}

// Close releases the underlying connection.
func (w *WatchStream) Close() error { return w.body.Close() }

// Watch opens a streaming watch on Pods starting after resourceVersion.
func (c *Client) Watch(ctx context.Context, resourceVersion string) (*WatchStream, error) {
	req, err := c.newRequest(ctx, c.podsURL(true, resourceVersion))
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("k8sclient: watch request: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, &APIError{StatusCode: resp.StatusCode, Body: string(body)}
	}
	scanner := bufio.NewScanner(resp.Body)
	// Pod objects can be large; grow the scanner's buffer well past the
	// default 64KiB so a big Pod spec doesn't truncate a watch line.
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	return &WatchStream{body: resp.Body, scanner: scanner}, nil
}

// ListAndWatch runs the list-then-watch loop that every informer is built
// on: LIST once to seed the cache and capture a resourceVersion, then WATCH
// from that version, and on a 410 Gone (the version has been compacted out
// of etcd) relist and start over. handler is called for the initial list
// (as a series of ADDED events) and for every subsequent watch event.
//
// Sentinel does not use this in production (client-go's SharedInformer
// does the same thing, better tested); it exists to demonstrate that the
// watch protocol under the informer is not magic. See docs/watch-protocol.md.
func (c *Client) ListAndWatch(ctx context.Context, handler func(WatchEventType, *Pod)) error {
	for ctx.Err() == nil {
		list, err := c.List(ctx)
		if err != nil {
			return err
		}
		for i := range list.Items {
			handler(EventAdded, &list.Items[i])
		}
		rv := list.Metadata.ResourceVersion

		if err := c.watchFrom(ctx, rv, handler); err != nil {
			var apiErr *APIError
			if errors.As(err, &apiErr) && apiErr.Gone() {
				// resourceVersion too old: relist from scratch.
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			// Transient stream error: brief backoff, then relist.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}
		}
	}
	return nil
}

func (c *Client) watchFrom(ctx context.Context, rv string, handler func(WatchEventType, *Pod)) error {
	stream, err := c.Watch(ctx, rv)
	if err != nil {
		return err
	}
	defer stream.Close()
	for {
		evt, err := stream.Next()
		if err != nil {
			return err
		}
		switch evt.Type {
		case EventError:
			st, _ := evt.AsStatus()
			return &APIError{StatusCode: st.Code, Body: st.Message}
		case EventBookmark:
			// resourceVersion checkpoint only; nothing to hand to the handler.
		default:
			pod, err := evt.AsPod()
			if err != nil {
				return fmt.Errorf("k8sclient: decoding %s event object: %w", evt.Type, err)
			}
			handler(evt.Type, &pod)
		}
	}
}
