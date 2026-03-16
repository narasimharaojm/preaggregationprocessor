// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package filter // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor/internal/filter"

import (
	"strings"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/filterset"
)

// AggregationType represents the type of aggregation to perform
type AggregationType int

const (
	AggMin AggregationType = iota
	AggMax
	AggSum
	AggCount
	AggAvg
	AggUnknown
)

// String returns the string representation of the aggregation type
func (at AggregationType) String() string {
	switch at {
	case AggMin:
		return "min"
	case AggMax:
		return "max"
	case AggSum:
		return "sum"
	case AggCount:
		return "count"
	case AggAvg:
		return "avg"
	default:
		return "unknown"
	}
}

// RoutingPatterns defines suffix patterns for detecting aggregation types
type RoutingPatterns struct {
	MinSuffix   string
	MaxSuffix   string
	SumSuffix   string
	CountSuffix string
	AvgSuffix   string

	// Pattern-based routing
	MinPatterns   filterset.FilterSet
	MaxPatterns   filterset.FilterSet
	SumPatterns   filterset.FilterSet
	CountPatterns filterset.FilterSet
	AvgPatterns   filterset.FilterSet

	// Type-based routing
	EnableTypeBased     bool
	GaugeDefaultAgg     AggregationType
	CounterDefaultAgg   string // "sum", "count", or "both"
	HistogramDefaultAgg AggregationType
	SummaryDefaultAgg   AggregationType
}

// Filter handles metric filtering and routing based on patterns
type Filter struct {
	includeFS filterset.FilterSet
	excludeFS filterset.FilterSet
	routing   RoutingPatterns
}

// NewFilter creates a new Filter instance
func NewFilter(includeFS, excludeFS filterset.FilterSet, routing RoutingPatterns) *Filter {
	return &Filter{
		includeFS: includeFS,
		excludeFS: excludeFS,
		routing:   routing,
	}
}

// ShouldProcess determines if a metric should be processed based on include/exclude patterns
func (f *Filter) ShouldProcess(metricName string) bool {
	// If include filter is defined, metric must match
	included := f.includeFS == nil || f.includeFS.Matches(metricName)

	// If exclude filter is defined, metric must not match
	excluded := f.excludeFS != nil && f.excludeFS.Matches(metricName)

	return included && !excluded
}

// DetectAggType detects the aggregation type from the metric name suffix
// and returns the type along with the base metric name (suffix removed)
func (f *Filter) DetectAggType(metricName string) (AggregationType, string) {
	// Priority 1: Check metric-specific patterns first
	if f.routing.MinPatterns != nil && f.routing.MinPatterns.Matches(metricName) {
		return AggMin, metricName
	}
	if f.routing.MaxPatterns != nil && f.routing.MaxPatterns.Matches(metricName) {
		return AggMax, metricName
	}
	if f.routing.SumPatterns != nil && f.routing.SumPatterns.Matches(metricName) {
		return AggSum, metricName
	}
	if f.routing.CountPatterns != nil && f.routing.CountPatterns.Matches(metricName) {
		return AggCount, metricName
	}
	if f.routing.AvgPatterns != nil && f.routing.AvgPatterns.Matches(metricName) {
		return AggAvg, metricName
	}

	// Priority 2: Check suffix-based patterns
	//if strings.HasSuffix(metricName, f.routing.MinSuffix) {
	//	baseName := strings.TrimSuffix(metricName, f.routing.MinSuffix)
	//	return AggMin, baseName
	//}
	//
	//if strings.HasSuffix(metricName, f.routing.MaxSuffix) {
	//	baseName := strings.TrimSuffix(metricName, f.routing.MaxSuffix)
	//	return AggMax, baseName
	//}
	//
	//if strings.HasSuffix(metricName, f.routing.SumSuffix) {
	//	baseName := strings.TrimSuffix(metricName, f.routing.SumSuffix)
	//	return AggSum, baseName
	//}
	//
	//if strings.HasSuffix(metricName, f.routing.CountSuffix) {
	//	baseName := strings.TrimSuffix(metricName, f.routing.CountSuffix)
	//	return AggCount, baseName
	//}
	//
	//if strings.HasSuffix(metricName, f.routing.AvgSuffix) {
	//	baseName := strings.TrimSuffix(metricName, f.routing.AvgSuffix)
	//	return AggAvg, baseName
	//}

	// Priority 3: Type-based defaults (requires metric type to be passed separately)
	// This will be handled by DetectAggTypeByMetricType
	return AggUnknown, metricName
}

// DetectAggTypeByMetricType detects aggregation type based on the metric type
// Used when no pattern or suffix matches
func (f *Filter) DetectAggTypeByMetricType(metricName string, metricType pmetric.MetricType) (AggregationType, string) {
	// First try pattern and suffix-based detection
	aggType, baseName := f.DetectAggType(metricName)
	if aggType != AggUnknown {
		return aggType, baseName
	}

	// If type-based routing is not enabled, return unknown
	if !f.routing.EnableTypeBased {
		return AggUnknown, metricName
	}

	// Apply type-based defaults
	switch metricType {
	case pmetric.MetricTypeGauge:
		if f.routing.GaugeDefaultAgg != AggUnknown {
			return f.routing.GaugeDefaultAgg, metricName
		}
	case pmetric.MetricTypeSum:
		// For sum metrics, we need to determine if it's a counter
		// This is a simplification - in reality, we'd check if it's monotonic
		// For now, use the counter default
		if f.routing.CounterDefaultAgg != "" {
			// Counter default is handled specially in the processor
			// Return a special marker that the processor will handle
			return AggUnknown, metricName
		}
	case pmetric.MetricTypeHistogram:
		if f.routing.HistogramDefaultAgg != AggUnknown {
			return f.routing.HistogramDefaultAgg, metricName
		}
	case pmetric.MetricTypeSummary:
		if f.routing.SummaryDefaultAgg != AggUnknown {
			return f.routing.SummaryDefaultAgg, metricName
		}
	}

	return AggUnknown, metricName
}

// GetCounterAggTypes returns the aggregation types for counter metrics
// when type-based routing is enabled
func (f *Filter) GetCounterAggTypes() []AggregationType {
	if !f.routing.EnableTypeBased {
		return nil
	}

	switch f.routing.CounterDefaultAgg {
	case "sum":
		return []AggregationType{AggSum}
	case "count":
		return []AggregationType{AggCount}
	case "both":
		return []AggregationType{AggSum, AggCount}
	default:
		return nil
	}
}

// GetGaugeDefaultAgg returns the default aggregation type for gauge metrics
// when type-based routing is enabled
func (f *Filter) GetGaugeDefaultAgg() AggregationType {
	if !f.routing.EnableTypeBased {
		return AggUnknown
	}
	return f.routing.GaugeDefaultAgg
}

// MatchesHistogramPrefix checks if a metric name matches any of the histogram prefixes
func MatchesHistogramPrefix(metricName string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(metricName, prefix) {
			return true
		}
	}
	return false
}
