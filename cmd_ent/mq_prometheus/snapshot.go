package main

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/common/expfmt"
)

const (
	minSnapshotTTL = 2 * time.Minute
	maxSnapshotTTL = 15 * time.Minute
)

type snapshotState struct {
	body                     []byte
	published, attempt       time.Time
	ttl, collectionDuration  time.Duration
	connected, success, busy bool
	failures                 uint64
}

// Only the worker publishes. HTTP requests load a single immutable generation.
type snapshotCache struct {
	current  atomic.Pointer[snapshotState]
	interval time.Duration
}

func newSnapshotCache(interval time.Duration) *snapshotCache {
	cache := &snapshotCache{interval: interval}
	cache.publish(snapshotState{ttl: minSnapshotTTL})
	return cache
}

func (cache *snapshotCache) publish(state snapshotState) {
	cache.current.Store(&state)
}

func (cache *snapshotCache) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	state := cache.current.Load()
	now := time.Now()
	age := time.Duration(0)
	if !state.published.IsZero() {
		age = now.Sub(state.published)
	}
	ttl := state.ttl
	if state.busy && now.Sub(state.attempt) > cache.interval/2 {
		ttl = minSnapshotTTL
	}
	stale := state.published.IsZero() || age >= ttl

	w.Header().Set("Content-Type", string(expfmt.FmtText))
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodHead {
		return
	}
	if len(state.body) != 0 {
		if _, err := w.Write(state.body); err != nil {
			return
		}
	}
	healthGauge(w, "connected", "Whether the MQ worker has discovered and connected to the queue manager.", boolFloat(state.connected))
	healthGauge(w, "collection_success", "Whether the last completed collection attempt succeeded.", boolFloat(state.success))
	healthGauge(w, "collection_in_progress", "Whether the MQ worker is currently collecting or connecting.", boolFloat(state.busy))
	healthGauge(w, "collection_duration_seconds", "Duration of the last completed collection attempt.", state.collectionDuration.Seconds())
	healthGauge(w, "last_attempt_timestamp_seconds", "Unix time when the last collection attempt started, or zero before startup.", unixTime(state.attempt))
	healthGauge(w, "snapshot_timestamp_seconds", "Unix time when the last complete snapshot was published, or zero before the first success.", unixTime(state.published))
	healthGauge(w, "snapshot_age_seconds", "Seconds since the last snapshot was published, or zero before the first success.", age.Seconds())
	healthGauge(w, "snapshot_ttl_seconds", "Effective freshness limit for the cached snapshot.", ttl.Seconds())
	healthGauge(w, "snapshot_stale", "Whether the snapshot is expired or has never been published.", boolFloat(stale))
	fmt.Fprintf(w, "# HELP mq_prometheus_ent_collection_errors_total Failed MQ worker attempts.\n# TYPE mq_prometheus_ent_collection_errors_total counter\nmq_prometheus_ent_collection_errors_total %d\n", state.failures)
}

func healthGauge(w http.ResponseWriter, name, help string, value float64) {
	fmt.Fprintf(w, "# HELP mq_prometheus_ent_%[1]s %[2]s\n# TYPE mq_prometheus_ent_%[1]s gauge\nmq_prometheus_ent_%[1]s %[3]g\n", name, help, value)
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func unixTime(value time.Time) float64 {
	if value.IsZero() {
		return 0
	}
	return float64(value.Unix())
}

type freshnessPolicy struct {
	streak int
}

func (policy *freshnessPolicy) next(ttl, elapsed, interval time.Duration, stable bool) time.Duration {
	if !stable || elapsed > interval/2 {
		policy.streak = 0
		return minSnapshotTTL
	}
	policy.streak++
	if policy.streak == 5 {
		policy.streak = 0
		return min(ttl+time.Minute, maxSnapshotTTL)
	}
	return ttl
}
