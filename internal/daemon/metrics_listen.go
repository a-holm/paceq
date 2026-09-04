package daemon

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/obs"
)

// ValidateMetricsListen refuses any --metrics-listen address that is not
// loopback (#40, 08 section 6). The metrics endpoint is an opt-in TCP
// surface: an operator who asks for one asked for loopback or made a
// mistake, and a mistake here must end in an explanation, not in a daemon
// listening on 0.0.0.0. Hostnames are resolved, so "localhost" works and
// anything that resolves partly off the loopback is refused.
func ValidateMetricsListen(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("metrics-listen %q is not host:port: %w", addr, err)
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return fmt.Errorf("metrics-listen %q names no host; give the loopback address explicitly, e.g. 127.0.0.1:9753", addr)
	}
	var ips []net.IP
	if ip := net.ParseIP(host); ip != nil {
		ips = []net.IP{ip}
	} else {
		resolved, err := net.LookupIP(host)
		if err != nil {
			return fmt.Errorf("metrics-listen host %q could not be resolved: %w", host, err)
		}
		ips = resolved
	}
	if len(ips) == 0 {
		return fmt.Errorf("metrics-listen host %q resolves to no addresses", host)
	}
	for _, ip := range ips {
		if !ip.IsLoopback() {
			return fmt.Errorf("metrics-listen %q would bind beyond loopback (%s); the metrics surface stays on the unix socket unless you explicitly name a loopback address such as 127.0.0.1:9753",
				addr, ip)
		}
	}
	return nil
}

// startMetricsTCP serves /metrics on the operator's opt-in loopback bind.
// The address is validated again here even though the CLI already did: the
// daemon must not trust that every caller asked. A bind that cannot start
// fails the daemon loudly, the same way a configured unix socket that cannot
// listen does: an operator who believes metrics are flowing must not discover
// otherwise from an empty graph.
func startMetricsTCP(addr string, collector *obs.Collector) error {
	if addr == "" || collector == nil {
		return nil
	}
	if err := ValidateMetricsListen(addr); err != nil {
		return err
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("the metrics endpoint could not listen on %s: %w", addr, err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		body := collector.Scrape(r.Context())
		w.Header().Set("Content-Type", obs.ContentType)
		_, _ = w.Write(body)
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(l) }() // lives with the process; Stop drains the group around it
	return nil
}
