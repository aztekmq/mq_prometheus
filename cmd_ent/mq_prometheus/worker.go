package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/ibm-messaging/mq-golang/v5/ibmmq"
	"github.com/ibm-messaging/mq-golang/v5/mqmetric"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	log "github.com/sirupsen/logrus"
)

var discoverConfig mqmetric.DiscoverConfig

func openExporter() (*exporter, error) {
	config.cf.CC.SingleConnect = config.keepRunning
	err := mqmetric.InitConnection(config.cf.QMgrName, config.cf.ReplyQ, config.cf.ReplyQ2, &config.cf.CC)
	if err != nil {
		var reply mqmetric.MQMetricError
		if !errors.As(err, &reply) || reply.MQReturn.MQRC == ibmmq.MQRC_STANDBY_Q_MGR ||
			reply.MQReturn.MQCC != ibmmq.MQCC_WARNING {
			return nil, err
		}
		log.Warn(err)
	}
	if config.cf.QMgrName == "" || strings.HasPrefix(config.cf.QMgrName, "*") {
		config.cf.QMgrName = mqmetric.GetResolvedQMgrName()
	}
	mqmetric.SetLocale(config.cf.Locale)
	discoverConfig.MonitoredQueues.ObjectNames = config.cf.MonitoredQueues
	discoverConfig.MonitoredQueues.SubscriptionSelector = strings.ToUpper(config.cf.QueueSubscriptionSelector)
	discoverConfig.MonitoredQueues.UseWildcard = config.cf.MetaPrefix == ""
	discoverConfig.MetaPrefix = config.cf.MetaPrefix
	if err := mqmetric.DiscoverAndSubscribe(discoverConfig); err != nil {
		return nil, err
	}
	if err := mqmetric.RediscoverAttributes(ibmmq.MQOT_CHANNEL, config.cf.MonitoredChannels); err != nil {
		log.Debugf("Initial channel discovery: %v", err)
	}
	if err := mqmetric.RediscoverAttributes(mqmetric.OT_CHANNEL_MQTT, config.cf.MonitoredMQTTChannels); err != nil {
		log.Debugf("Initial MQTT channel discovery: %v", err)
	}
	if code, err := mqmetric.VerifyConfig(); err != nil {
		if code != ibmmq.MQCC_WARNING {
			return nil, err
		}
		log.Warn(err)
	}
	supportsHostnameLabelVal = nil
	platformString = strings.TrimPrefix(ibmmq.MQItoString("PL", int(mqmetric.GetPlatform())), "MQPL_")
	lastQueueDiscovery = time.Now()
	allocateAllGauges()
	log.Infof("Monitoring queue manager %s on %s", config.cf.QMgrName, platformString)
	return newExporter(), nil
}

func runWorker(ctx context.Context, cache *snapshotCache) error {
	state := *cache.current.Load()
	policy := freshnessPolicy{}
	var source *exporter
	interval := cache.interval
	initialBackoff := config.reconnectIntervalDuration
	maxBackoff := max(initialBackoff, 2*time.Minute)
	backoff := initialBackoff
	defer func() {
		if source != nil {
			mqmetric.EndConnection()
		}
	}()

	for ctx.Err() == nil {
		reconnecting := source == nil
		state.attempt = time.Now()
		state.busy = true
		cache.publish(state)

		var err error
		if reconnecting {
			source, err = openExporter()
			if err != nil {
				mqmetric.EndConnection()
			}
		}
		state.connected = source != nil
		cache.publish(state)

		var body []byte
		var generated time.Time
		if err == nil && ctx.Err() == nil {
			body, generated, err = collectSnapshot(source.collect)
		}
		if ctx.Err() != nil {
			return nil
		}

		now := time.Now()
		state.collectionDuration = now.Sub(state.attempt)
		state.success = err == nil
		state.busy = false
		// Skip missed slots. A slow cycle never queues more work or triggers a
		// second cycle immediately on completion.
		delay := interval - state.collectionDuration%interval
		if err != nil {
			state.failures++
			state.ttl = policy.next(state.ttl, state.collectionDuration, interval, false)
			log.Errorf("MQ worker attempt failed for %s: %v", config.cf.QMgrName, err)
			if source != nil && mqConnectionError(err) {
				state.connected = false
				cache.publish(state)
				mqmetric.EndConnection()
				source = nil
			}
			if source == nil {
				delay = min(maxBackoff, backoff-time.Duration(rand.Int64N(int64(backoff/5)+1)))
				backoff += min(backoff, maxBackoff-backoff)
			}
		} else {
			stable := !reconnecting && !state.published.IsZero() && now.Sub(state.published) < state.ttl
			state.ttl = policy.next(state.ttl, state.collectionDuration, interval, stable)
			state.body = body
			state.published = generated
			backoff = initialBackoff
		}
		cache.publish(state)
		if err != nil && source == nil && !config.keepRunning {
			return fmt.Errorf("queue manager %s unavailable: %w", config.cf.QMgrName, err)
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	return nil
}

func mqConnectionError(err error) bool {
	var metricErr mqmetric.MQMetricError
	var reply *ibmmq.MQReturn
	if errors.As(err, &metricErr) {
		return metricErr.MQReturn.MQRC != ibmmq.MQRC_NONE
	}
	if errors.As(err, &reply) {
		return reply.MQRC != ibmmq.MQRC_NONE
	}
	return false
}

type gatheredMetrics []prometheus.Metric

func (metrics gatheredMetrics) Describe(chan<- *prometheus.Desc) {}
func (metrics gatheredMetrics) Collect(ch chan<- prometheus.Metric) {
	for _, metric := range metrics {
		ch <- metric
	}
}

func collectSnapshot(collect func(chan<- prometheus.Metric) error) ([]byte, time.Time, error) {
	out := make(chan prometheus.Metric, 256)
	done := make(chan gatheredMetrics, 1)
	go func() {
		var metrics gatheredMetrics
		for metric := range out {
			metrics = append(metrics, metric)
		}
		done <- metrics
	}()
	err := collect(out)
	close(out)
	metrics := <-done
	if err != nil {
		return nil, time.Time{}, err
	}

	// Gather only after MQ collection has finished. No GaugeVec or CounterVec is
	// accessed by an HTTP handler or mutated before the bytes have been encoded.
	registry := prometheus.NewRegistry()
	registry.MustRegister(metrics)
	families, err := (prometheus.Gatherers{registry, prometheus.DefaultGatherer}).Gather()
	if err != nil {
		return nil, time.Time{}, err
	}
	generated := time.Now()
	body, err := encodeSnapshot(families, generated)
	return body, generated, err
}

const availabilitySuffix = "_available_timestamp_seconds"

func encodeSnapshot(families []*dto.MetricFamily, generated time.Time) ([]byte, error) {
	names := make(map[string]struct{}, len(families))
	for _, family := range families {
		names[family.GetName()] = struct{}{}
		// Histogram and summary families emit these additional series names.
		switch family.GetType() {
		case dto.MetricType_HISTOGRAM, dto.MetricType_SUMMARY:
			names[family.GetName()+"_sum"] = struct{}{}
			names[family.GetName()+"_count"] = struct{}{}
			if family.GetType() == dto.MetricType_HISTOGRAM {
				names[family.GetName()+"_bucket"] = struct{}{}
			}
		}
	}
	var body bytes.Buffer
	seconds := float64(generated.Unix())
	kind := dto.MetricType_GAUGE
	value := &dto.Gauge{Value: &seconds}
	for _, family := range families {
		name := family.GetName() + availabilitySuffix
		if _, collision := names[name]; collision {
			return nil, fmt.Errorf("availability timestamp metric %q collides with an existing series", name)
		}
		help := "Unix time when the exporter made " + family.GetName() + " available in this snapshot."
		companion := dto.MetricFamily{Name: &name, Help: &help, Type: &kind}
		companion.Metric = make([]*dto.Metric, 0, len(family.Metric))
		for _, metric := range family.Metric {
			companion.Metric = append(companion.Metric, &dto.Metric{Label: metric.Label, Gauge: value})
		}
		if _, err := expfmt.MetricFamilyToText(&body, family); err != nil {
			return nil, err
		}
		if _, err := expfmt.MetricFamilyToText(&body, &companion); err != nil {
			return nil, err
		}
	}
	return body.Bytes(), nil
}
