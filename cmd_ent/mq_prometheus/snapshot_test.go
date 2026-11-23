package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

func TestSnapshotScrapesDuringBlockedCollection(t *testing.T) {
	cache := newSnapshotCache(time.Minute)
	initial := *cache.current.Load()
	initial.body = []byte("# TYPE test_queue_depth gauge\ntest_queue_depth 7\n")
	initial.published = time.Now().Add(-3 * time.Minute)
	initial.attempt = time.Now().Add(-40 * time.Second)
	initial.ttl = maxSnapshotTTL
	initial.connected = true
	initial.busy = true
	cache.publish(initial)

	entered := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)
	stopErr := errors.New("collection aborted")
	go func() {
		body, _, err := collectSnapshot(func(out chan<- prometheus.Metric) error {
			close(entered)
			<-release
			out <- prometheus.MustNewConstMetric(
				prometheus.NewDesc("test_queue_depth", "New depth", nil, nil), prometheus.GaugeValue, 9)
			return stopErr
		})
		if body != nil {
			result <- errors.New("partial snapshot returned")
			return
		}
		result <- err
	}()
	defer close(release)
	<-entered

	var group sync.WaitGroup
	for range 16 {
		group.Go(func() {
			rec := httptest.NewRecorder()
			cache.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			body := rec.Body.String()
			for _, expected := range []string{
				"test_queue_depth 7\n", "mq_prometheus_ent_collection_in_progress 1\n",
				"mq_prometheus_ent_snapshot_ttl_seconds 120\n", "mq_prometheus_ent_snapshot_stale 1\n",
			} {
				if !strings.Contains(body, expected) || rec.Code != http.StatusOK {
					t.Errorf("status %d, missing %q", rec.Code, expected)
				}
			}
		})
	}
	group.Wait()
	release <- struct{}{}
	if err := <-result; !errors.Is(err, stopErr) {
		t.Fatalf("expected aborted collection, got %v", err)
	}
}

func TestFreshnessPolicy(t *testing.T) {
	policy := freshnessPolicy{}
	ttl := minSnapshotTTL
	for i := range 80 {
		ttl = policy.next(ttl, time.Second, time.Minute, true)
		want := min(minSnapshotTTL+time.Duration((i+1)/5)*time.Minute, maxSnapshotTTL)
		if ttl != want {
			t.Fatalf("cycle %d: got %s, want %s", i+1, ttl, want)
		}
	}
	if got := policy.next(ttl, 31*time.Second, time.Minute, true); got != minSnapshotTTL {
		t.Fatalf("slow collection TTL = %s", got)
	}
	if got := policy.next(ttl, time.Second, time.Minute, false); got != minSnapshotTTL {
		t.Fatalf("failed or reconnecting collection TTL = %s", got)
	}
}

func TestSnapshotAvailabilityTimestamps(t *testing.T) {
	name, label, queue := "test_queue_depth", "queue", "ORDERS"
	depth := float64(7)
	kind := dto.MetricType_GAUGE
	family := &dto.MetricFamily{
		Name: &name, Type: &kind,
		Metric: []*dto.Metric{{
			Label: []*dto.LabelPair{{Name: &label, Value: &queue}},
			Gauge: &dto.Gauge{Value: &depth},
		}},
	}
	generated := time.Unix(1795280400, 0)
	body, err := encodeSnapshot([]*dto.MetricFamily{family}, generated)
	if err != nil {
		t.Fatal(err)
	}
	parser := expfmt.NewTextParser(model.LegacyValidation)
	parsed, err := parser.TextToMetricFamilies(strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	source := parsed[name].GetMetric()[0]
	timestamp := parsed[name+availabilitySuffix].GetMetric()[0]
	if source.GetGauge().GetValue() != depth || source.TimestampMs != nil {
		t.Fatalf("original sample was modified: %v", source)
	}
	if timestamp.GetGauge().GetValue() != float64(generated.Unix()) ||
		timestamp.GetLabel()[0].GetName() != label || timestamp.GetLabel()[0].GetValue() != queue ||
		timestamp.TimestampMs != nil {
		t.Fatalf("incorrect availability companion: %v", timestamp)
	}
	collision := name + availabilitySuffix
	if response, err := encodeSnapshot([]*dto.MetricFamily{family, {Name: &collision}}, generated); err == nil || response != nil {
		t.Fatalf("timestamp name collision was accepted: %v", err)
	}
}
