package daemon

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/obs"
	"github.com/a-holm/paceq/internal/store"
)

// TestValidateMetricsListen is binding test 8's refusal half: anything that
// would put the metrics surface beyond loopback is refused with an
// explanation that names what to do instead.
func TestValidateMetricsListen(t *testing.T) {
	cases := []struct {
		addr   string
		wantOK bool
	}{
		{"127.0.0.1:9753", true},
		{"[::1]:9753", true},
		{"localhost:9753", true},
		{"0.0.0.0:9753", false},      // every interface: the classic mistake
		{"192.168.1.10:9753", false}, // a LAN address
		{"example.com:9753", false},  // resolves off loopback
		{"9753", false},              // not host:port at all
		{":9753", false},             // names no host: wildcard by omission
	}
	for _, tc := range cases {
		err := ValidateMetricsListen(tc.addr)
		if tc.wantOK && err != nil {
			t.Errorf("ValidateMetricsListen(%q) = %v, want accepted", tc.addr, err)
		}
		if !tc.wantOK && err == nil {
			t.Errorf("ValidateMetricsListen(%q) accepted a non-loopback bind", tc.addr)
		}
	}
}

// metricsStubSource is the smallest obs.Source that renders a document; the
// TCP tests care about transport, not content breadth.
type metricsStubSource struct{}

func (m *metricsStubSource) MetricsIntegrityViolations(context.Context) ([]store.MetricsIntegrityViolation, error) {
	return nil, nil
}

func (m *metricsStubSource) MetricsFsckLastRun(context.Context) (time.Time, bool, error) {
	return time.Time{}, false, nil
}

func (m *metricsStubSource) MetricsRunsByStates(context.Context) ([]store.MetricsJobStateCount, error) {
	return nil, nil
}

func (m *metricsStubSource) MetricsLastSuccesses(context.Context) ([]store.MetricsJobStamp, error) {
	return nil, nil
}

func (m *metricsStubSource) MetricsJobSLAs(context.Context) ([]store.MetricsJobSLA, error) {
	return nil, nil
}

func (m *metricsStubSource) MetricsScheduleStates(context.Context) ([]store.MetricsInstigatorState, error) {
	return nil, nil
}

func (m *metricsStubSource) MetricsSensorStates(context.Context) ([]store.MetricsInstigatorState, error) {
	return nil, nil
}

func (m *metricsStubSource) MetricsTickLags(context.Context) ([]store.MetricsSourceLag, error) {
	return nil, nil
}

func (m *metricsStubSource) MetricsLastTicks(context.Context) ([]store.MetricsSourceStamp, error) {
	return nil, nil
}

func (m *metricsStubSource) MetricsOutageSeconds(context.Context) (float64, error) {
	return 0, nil
}

func (m *metricsStubSource) MetricsMetaValues(context.Context) (map[string]string, error) {
	return map[string]string{}, nil
}

func (m *metricsStubSource) MetricsDBBytes(context.Context) (int64, int64, error) {
	return 0, 0, nil
}
func (m *metricsStubSource) TakeWriteWaitMax() float64 { return 0 }
func (m *metricsStubSource) BusyTotal() uint64         { return 0 }
func (m *metricsStubSource) MetricsNotificationsPending(context.Context) (int64, error) {
	return 0, nil
}

func (m *metricsStubSource) MetricsNotificationsGivenUp(context.Context) (int64, error) {
	return 0, nil
}

func (m *metricsStubSource) TakeDelivery() store.DeliverySnapshot {
	return store.DeliverySnapshot{}
}

var _ obs.Source = (*metricsStubSource)(nil)

// TestMetricsTCPAnswersOnLoopback is binding test 8's acceptance half: an
// explicit loopback bind serves /metrics with the exposition content type.
func TestMetricsTCPAnswersOnLoopback(t *testing.T) {
	src := &metricsStubSource{}
	counters := obs.NewCounters()
	collector := obs.NewCollector(src, counters, clock.NewFake(time.Now()),
		obs.Identity{Version: "v", Commit: "c", GoVersion: "go"}, "")

	// Port 0: the kernel picks one, so the test never races a real listener.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe a loopback port: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close the probe listener: %v", err)
	}
	if err := startMetricsTCP(addr, collector); err != nil {
		t.Fatalf("start metrics TCP on %s: %v", addr, err)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("GET http://%s/metrics: %v", addr, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if ct := resp.Header.Get("Content-Type"); ct != obs.ContentType {
		t.Errorf("Content-Type = %q, want %q", ct, obs.ContentType)
	}
	if !strings.Contains(string(body), "pulseq_build_info") {
		t.Error("/metrics over TCP must carry the exposition")
	}
}

// TestStartMetricsTCPRefusesBeyondLoopback keeps Serve honest on its own: no
// caller path can talk it into binding beyond loopback, whatever the CLI
// already checked.
func TestStartMetricsTCPRefusesBeyondLoopback(t *testing.T) {
	if err := startMetricsTCP("0.0.0.0:9753", &obs.Collector{}); err == nil {
		t.Fatal("a wildcard bind must be refused before listening")
	}
}
