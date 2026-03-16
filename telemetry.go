// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package preaggregationprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor"

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	meterName = "github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor"
)

// telemetry holds all the metric instruments for the processor
type telemetry struct {
	// Metrics processing counters
	metricsReceived      metric.Int64Counter
	metricsAggregated    metric.Int64Counter
	metricsPassthrough   metric.Int64Counter
	metricsEmitted       metric.Int64Counter
	datapointsProcessed  metric.Int64Counter

	// Pattern matching metrics
	patternMatches metric.Int64Counter
	patternMisses  metric.Int64Counter
	typeBasedRoute metric.Int64Counter

	// Aggregation state metrics
	activeStates     metric.Int64ObservableGauge
	statesCreated    metric.Int64Counter
	statesExpired    metric.Int64Counter
	cumulativeStates metric.Int64ObservableGauge

	// Window processing metrics
	windowFlushes      metric.Int64Counter
	windowFlushDuration metric.Float64Histogram
	windowMetricsCount metric.Int64Histogram

	// Performance metrics
	processingDuration metric.Float64Histogram

	// Error metrics
	errors                metric.Int64Counter
	globConversionErrors  metric.Int64Counter
	emissionFailures      metric.Int64Counter

	// Mode-specific metrics (Statistical)
	cumulativeToDelta metric.Int64Counter
	deltaValuesSkipped metric.Int64Counter

	// Efficiency metrics
	cardinalityReduction metric.Int64ObservableGauge

	// Reference to processor for observable callbacks
	processor *preAggregationProcessor
}

// newTelemetry creates and initializes all telemetry instruments
func newTelemetry(meter metric.Meter, processor *preAggregationProcessor) (*telemetry, error) {
	t := &telemetry{
		processor: processor,
	}

	var err error

	// Metrics processing counters
	t.metricsReceived, err = meter.Int64Counter(
		"otelcol_processor_preaggregation_metrics_received",
		metric.WithDescription("Total number of metrics received by the processor"),
		metric.WithUnit("{metrics}"),
	)
	if err != nil {
		return nil, err
	}

	t.metricsAggregated, err = meter.Int64Counter(
		"otelcol_processor_preaggregation_metrics_aggregated",
		metric.WithDescription("Total number of metrics aggregated"),
		metric.WithUnit("{metrics}"),
	)
	if err != nil {
		return nil, err
	}

	t.metricsPassthrough, err = meter.Int64Counter(
		"otelcol_processor_preaggregation_metrics_passthrough",
		metric.WithDescription("Total number of metrics passed through without aggregation"),
		metric.WithUnit("{metrics}"),
	)
	if err != nil {
		return nil, err
	}

	t.metricsEmitted, err = meter.Int64Counter(
		"otelcol_processor_preaggregation_metrics_emitted",
		metric.WithDescription("Total number of metrics emitted after aggregation"),
		metric.WithUnit("{metrics}"),
	)
	if err != nil {
		return nil, err
	}

	t.datapointsProcessed, err = meter.Int64Counter(
		"otelcol_processor_preaggregation_datapoints_processed",
		metric.WithDescription("Total number of datapoints processed"),
		metric.WithUnit("{datapoints}"),
	)
	if err != nil {
		return nil, err
	}

	// Pattern matching metrics
	t.patternMatches, err = meter.Int64Counter(
		"otelcol_processor_preaggregation_pattern_matches",
		metric.WithDescription("Total number of pattern matches"),
		metric.WithUnit("{matches}"),
	)
	if err != nil {
		return nil, err
	}

	t.patternMisses, err = meter.Int64Counter(
		"otelcol_processor_preaggregation_pattern_misses",
		metric.WithDescription("Total number of pattern misses"),
		metric.WithUnit("{misses}"),
	)
	if err != nil {
		return nil, err
	}

	t.typeBasedRoute, err = meter.Int64Counter(
		"otelcol_processor_preaggregation_routing_type_based",
		metric.WithDescription("Total number of type-based routing decisions"),
		metric.WithUnit("{routes}"),
	)
	if err != nil {
		return nil, err
	}

	// Aggregation state metrics
	t.activeStates, err = meter.Int64ObservableGauge(
		"otelcol_processor_preaggregation_states_active",
		metric.WithDescription("Current number of active aggregation states"),
		metric.WithUnit("{states}"),
		metric.WithInt64Callback(t.observeActiveStates),
	)
	if err != nil {
		return nil, err
	}

	t.statesCreated, err = meter.Int64Counter(
		"otelcol_processor_preaggregation_states_created",
		metric.WithDescription("Total number of aggregation states created"),
		metric.WithUnit("{states}"),
	)
	if err != nil {
		return nil, err
	}

	t.statesExpired, err = meter.Int64Counter(
		"otelcol_processor_preaggregation_states_expired",
		metric.WithDescription("Total number of aggregation states expired/cleaned up"),
		metric.WithUnit("{states}"),
	)
	if err != nil {
		return nil, err
	}

	t.cumulativeStates, err = meter.Int64ObservableGauge(
		"otelcol_processor_preaggregation_cumulative_tracking_states",
		metric.WithDescription("Current number of cumulative-to-delta tracking states (persists across windows)"),
		metric.WithUnit("{states}"),
		metric.WithInt64Callback(t.observeCumulativeStates),
	)
	if err != nil {
		return nil, err
	}

	// Window processing metrics
	t.windowFlushes, err = meter.Int64Counter(
		"otelcol_processor_preaggregation_window_flushes",
		metric.WithDescription("Total number of window flush operations"),
		metric.WithUnit("{flushes}"),
	)
	if err != nil {
		return nil, err
	}

	t.windowFlushDuration, err = meter.Float64Histogram(
		"otelcol_processor_preaggregation_window_flush_duration",
		metric.WithDescription("Duration of window flush operations"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	t.windowMetricsCount, err = meter.Int64Histogram(
		"otelcol_processor_preaggregation_window_metrics_count",
		metric.WithDescription("Number of metrics processed per window flush"),
		metric.WithUnit("{metrics}"),
	)
	if err != nil {
		return nil, err
	}

	// Performance metrics
	t.processingDuration, err = meter.Float64Histogram(
		"otelcol_processor_preaggregation_processing_duration",
		metric.WithDescription("Duration of metric processing operations"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	// Error metrics
	t.errors, err = meter.Int64Counter(
		"otelcol_processor_preaggregation_errors",
		metric.WithDescription("Total number of errors"),
		metric.WithUnit("{errors}"),
	)
	if err != nil {
		return nil, err
	}

	t.globConversionErrors, err = meter.Int64Counter(
		"otelcol_processor_preaggregation_glob_conversion_errors",
		metric.WithDescription("Total number of glob pattern conversion errors"),
		metric.WithUnit("{errors}"),
	)
	if err != nil {
		return nil, err
	}

	t.emissionFailures, err = meter.Int64Counter(
		"otelcol_processor_preaggregation_emission_failures",
		metric.WithDescription("Total number of metric emission failures"),
		metric.WithUnit("{failures}"),
	)
	if err != nil {
		return nil, err
	}

	// Statistical mode metrics
	t.cumulativeToDelta, err = meter.Int64Counter(
		"otelcol_processor_preaggregation_cumulative_to_delta_conversions",
		metric.WithDescription("Total number of cumulative to delta conversions"),
		metric.WithUnit("{conversions}"),
	)
	if err != nil {
		return nil, err
	}

	t.deltaValuesSkipped, err = meter.Int64Counter(
		"otelcol_processor_preaggregation_delta_values_skipped",
		metric.WithDescription("Total number of delta values skipped (zero or negative)"),
		metric.WithUnit("{values}"),
	)
	if err != nil {
		return nil, err
	}

	// Efficiency metrics
	t.cardinalityReduction, err = meter.Int64ObservableGauge(
		"otelcol_processor_preaggregation_cardinality_reduction",
		metric.WithDescription("Cardinality reduction (input - output datapoints in current window)"),
		metric.WithUnit("{datapoints}"),
		metric.WithInt64Callback(t.observeCardinalityReduction),
	)
	if err != nil {
		return nil, err
	}

	return t, nil
}

// observeActiveStates is a callback for the active states gauge
func (t *telemetry) observeActiveStates(ctx context.Context, observer metric.Int64Observer) error {
	if t.processor == nil || t.processor.statisticalAgg == nil {
		return nil
	}

	// Count active states by metric name and aggregation type
	metricCounts := t.processor.statisticalAgg.GetStateCountsByMetric()

	// Emit per-metric observations
	for metricName, aggCounts := range metricCounts {
		for aggType, count := range aggCounts {
			observer.Observe(count, metric.WithAttributes(
				attribute.String("agg_type", aggType.String()),
				attribute.String("metric_name", metricName),
			))
		}
	}

	return nil
}

// observeCumulativeStates is a callback for the cumulative tracking states gauge
func (t *telemetry) observeCumulativeStates(ctx context.Context, observer metric.Int64Observer) error {
	if t.processor == nil || t.processor.statisticalAgg == nil {
		return nil
	}

	count := t.processor.statisticalAgg.GetCumulativeStateCount()
	observer.Observe(count)

	return nil
}

// observeCardinalityReduction is a callback for the cardinality reduction gauge
func (t *telemetry) observeCardinalityReduction(ctx context.Context, observer metric.Int64Observer) error {
	if t.processor == nil {
		return nil
	}

	// This would need to be tracked during window processing
	// For now, we'll implement this as a counter difference
	reduction := t.processor.getCardinalityReduction()
	observer.Observe(reduction)

	return nil
}
