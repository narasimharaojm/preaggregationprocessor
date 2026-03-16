// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package emitter // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor/internal/emitter"

import (
	"encoding/hex"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor/internal/aggregator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor/internal/filter"
)

// Emitter creates OTLP metrics from aggregated data
type Emitter struct {
	aggregationTemporality pmetric.AggregationTemporality

	// Pooled lookup maps for deduplication (reused across flushes)
	rmLookup map[resourceKey]pmetric.ResourceMetrics
	smLookup map[scopeKey]pmetric.ScopeMetrics
}

// resourceKey and scopeKey types for map keys
type resourceKey string
type scopeKey string

// NewEmitter creates a new Emitter
func NewEmitter(aggregationTemporality pmetric.AggregationTemporality) *Emitter {
	return &Emitter{
		aggregationTemporality: aggregationTemporality,
		rmLookup:               make(map[resourceKey]pmetric.ResourceMetrics),
		smLookup:               make(map[scopeKey]pmetric.ScopeMetrics),
	}
}

// EmitStatistical creates OTLP metrics from statistical aggregations
func (e *Emitter) EmitStatistical(
	aggregated []aggregator.AggregatedMetric,
	windowStart pcommon.Timestamp,
	windowEnd pcommon.Timestamp,
) pmetric.Metrics {
	md := pmetric.NewMetrics()

	if len(aggregated) == 0 {
		return md
	}

	// Clear pooled maps for reuse (avoids reallocation)
	for k := range e.rmLookup {
		delete(e.rmLookup, k)
	}
	for k := range e.smLookup {
		delete(e.smLookup, k)
	}

	for _, agg := range aggregated {
		// Generate resource key
		resHash := pdatautil.MapHash(agg.Identity.Resource.Attributes())
		resKey := resourceKey(hex.EncodeToString(resHash[:]))

		// Find or create ResourceMetrics
		rm, ok := e.rmLookup[resKey]
		if !ok {
			rm = md.ResourceMetrics().AppendEmpty()
			agg.Identity.Resource.Attributes().CopyTo(rm.Resource().Attributes())
			e.rmLookup[resKey] = rm
		}

		// Generate scope key
		scopeKeyStr := scopeKey(string(resKey) + agg.Identity.Scope.Name() + agg.Identity.Scope.Version())

		// Find or create ScopeMetrics
		sm, ok := e.smLookup[scopeKeyStr]
		if !ok {
			sm = rm.ScopeMetrics().AppendEmpty()
			agg.Identity.Scope.CopyTo(sm.Scope())
			e.smLookup[scopeKeyStr] = sm
		}

		// Create metric
		m := sm.Metrics().AppendEmpty()
		m.SetName(agg.BaseName)
		m.SetUnit(agg.Identity.MetricUnit)

		// Set metric type based on aggregation type
		// Min/Max/Avg -> Gauge (snapshot values)
		// Sum/Count -> Sum (delta/cumulative values)
		switch agg.AggType {
		case filter.AggMin, filter.AggMax, filter.AggAvg:
			gauge := m.SetEmptyGauge()
			dp := gauge.DataPoints().AppendEmpty()
			dp.SetDoubleValue(agg.Value)
			dp.SetTimestamp(windowEnd)
			agg.Identity.Attributes.CopyTo(dp.Attributes())

		case filter.AggSum, filter.AggCount:
			sum := m.SetEmptySum()
			sum.SetAggregationTemporality(e.aggregationTemporality)
			sum.SetIsMonotonic(false) // Aggregated values are not necessarily monotonic

			dp := sum.DataPoints().AppendEmpty()
			dp.SetDoubleValue(agg.Value)
			dp.SetStartTimestamp(windowStart)
			dp.SetTimestamp(windowEnd)
			agg.Identity.Attributes.CopyTo(dp.Attributes())
		}
	}

	return md
}

