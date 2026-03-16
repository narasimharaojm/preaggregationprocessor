// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package preaggregationprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor"

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/filterset"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/filterset/regexp"
)

// Custom match type for glob patterns
const (
	MatchTypeGlob filterset.MatchType = "glob"
)

// ProcessingMode defines the mode of operation for the processor
type ProcessingMode string

const (
	// ModeStatistical performs statistical aggregations (min/max/sum/count/avg)
	ModeStatistical ProcessingMode = "statistical"
)

// Config defines configuration for the preaggregation processor
type Config struct {
	// Mode determines the processing mode: "statistical", "histogram", or "both"
	Mode ProcessingMode `mapstructure:"mode"`

	// Window configuration
	Window WindowConfig `mapstructure:"window"`

	// Metric filtering
	Include FilterConfig `mapstructure:"include"`
	Exclude FilterConfig `mapstructure:"exclude"`

	// Routing patterns for statistical mode
	RoutingPatterns RoutingConfig `mapstructure:"routing_patterns"`

	// State management
	MaxStaleness time.Duration `mapstructure:"max_staleness"`
}

// WindowConfig defines time window configuration
type WindowConfig struct {
	// Duration of the aggregation window
	Duration time.Duration `mapstructure:"duration"`

	// AggregationTemporality for emitted metrics
	AggregationTemporality string `mapstructure:"aggregation_temporality"`
}

// FilterConfig defines metric filtering configuration
type FilterConfig struct {
	filterset.Config `mapstructure:",squash"`
	// Patterns for metric name matching
	Patterns []string `mapstructure:"patterns"`
	// RegexpConfig for regex caching (same as filter processor)
	RegexpConfig *regexp.Config `mapstructure:"regexp"`
}

// RoutingConfig defines suffix patterns for detecting aggregation types
type RoutingConfig struct {
	//MinSuffix   string `mapstructure:"min_suffix"`
	//MaxSuffix   string `mapstructure:"max_suffix"`
	//SumSuffix   string `mapstructure:"sum_suffix"`
	//CountSuffix string `mapstructure:"count_suffix"`
	//AvgSuffix   string `mapstructure:"avg_suffix"`

	// MatchType determines how patterns are matched against metric names.
	// Valid values: "glob", "regexp" (default), "strict"
	// - "glob": Patterns use glob syntax with wildcards (e.g., "*.min", "cpu.*")
	// - "regexp": Patterns are regular expressions (e.g., ".*\\.min$")
	// - "strict": Patterns are exact string matches (e.g., "exact.metric.name")
	MatchType filterset.MatchType `mapstructure:"match_type"`

	// Metric patterns for each aggregation type (comma-separated)
	MinPatterns   []string `mapstructure:"min_patterns"`
	MaxPatterns   []string `mapstructure:"max_patterns"`
	SumPatterns   []string `mapstructure:"sum_patterns"`
	CountPatterns []string `mapstructure:"count_patterns"`
	AvgPatterns   []string `mapstructure:"avg_patterns"`

	// RegexpConfig for regex caching (only used when match_type is "regexp")
	RegexpConfig *regexp.Config `mapstructure:"regexp"`

	// Type-based default aggregation settings
	EnableTypeBased     bool   `mapstructure:"enable_type_based"`
	GaugeDefaultAgg     string `mapstructure:"gauge_default_agg"`     // Default: "avg"
	CounterDefaultAgg   string `mapstructure:"counter_default_agg"`   // "sum", "count", or "both"
	HistogramDefaultAgg string `mapstructure:"histogram_default_agg"` // Optional: how to handle histogram metrics
	SummaryDefaultAgg   string `mapstructure:"summary_default_agg"`   // Optional: how to handle summary metrics
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
	// Validate mode
	switch cfg.Mode {
	case ModeStatistical:
		// Valid modes
	default:
		return fmt.Errorf("invalid mode %q, must be one of: statistical", cfg.Mode)
	}

	// Validate window duration
	if cfg.Window.Duration <= 0 {
		return fmt.Errorf("window duration must be positive, got %v", cfg.Window.Duration)
	}

	// Validate aggregation temporality
	if cfg.Window.AggregationTemporality != "" {
		switch cfg.Window.AggregationTemporality {
		case "cumulative", "delta":
			// Valid temporality
		default:
			return fmt.Errorf("invalid aggregation_temporality %q, must be cumulative or delta", cfg.Window.AggregationTemporality)
		}
	}

	// Validate routing patterns for statistical mode
	if cfg.Mode == ModeStatistical {
		//if cfg.RoutingPatterns.MinSuffix == "" || cfg.RoutingPatterns.MaxSuffix == "" ||
		//	cfg.RoutingPatterns.SumSuffix == "" || cfg.RoutingPatterns.CountSuffix == "" ||
		//	cfg.RoutingPatterns.AvgSuffix == "" {
		//	return fmt.Errorf("all routing pattern suffixes must be specified for statistical mode")
		//}

		// Validate match_type if patterns are specified
		if len(cfg.RoutingPatterns.MinPatterns) > 0 || len(cfg.RoutingPatterns.MaxPatterns) > 0 ||
			len(cfg.RoutingPatterns.SumPatterns) > 0 || len(cfg.RoutingPatterns.CountPatterns) > 0 ||
			len(cfg.RoutingPatterns.AvgPatterns) > 0 {
			// Default to glob if not specified (user-friendly)
			if cfg.RoutingPatterns.MatchType == "" {
				cfg.RoutingPatterns.MatchType = MatchTypeGlob
			}
			// Validate match_type value
			switch cfg.RoutingPatterns.MatchType {
			case MatchTypeGlob, filterset.Regexp, filterset.Strict:
				// Valid match types
			default:
				return fmt.Errorf("invalid routing_patterns match_type %q, must be one of: glob, regexp, strict", cfg.RoutingPatterns.MatchType)
			}
		}

		// Validate type-based aggregation settings if enabled
		if cfg.RoutingPatterns.EnableTypeBased {
			// Validate gauge default aggregation
			if cfg.RoutingPatterns.GaugeDefaultAgg != "" {
				switch cfg.RoutingPatterns.GaugeDefaultAgg {
				case "avg":
					// Valid aggregation types
				default:
					return fmt.Errorf("invalid gauge_default_agg %q, must be one of: avg", cfg.RoutingPatterns.GaugeDefaultAgg)
				}
			}

			// Validate counter default aggregation
			if cfg.RoutingPatterns.CounterDefaultAgg != "" {
				switch cfg.RoutingPatterns.CounterDefaultAgg {
				case "sum", "count", "both":
					// Valid aggregation types for counters
				default:
					return fmt.Errorf("invalid counter_default_agg %q, must be one of: sum, count, both", cfg.RoutingPatterns.CounterDefaultAgg)
				}
			}

			// Validate histogram default aggregation if specified
			if cfg.RoutingPatterns.HistogramDefaultAgg != "" {
				switch cfg.RoutingPatterns.HistogramDefaultAgg {
				case "min", "max", "sum", "count", "avg", "passthrough":
					// Valid options
				default:
					return fmt.Errorf("invalid histogram_default_agg %q, must be one of: min, max, sum, count, avg, passthrough", cfg.RoutingPatterns.HistogramDefaultAgg)
				}
			}

			// Validate summary default aggregation if specified
			if cfg.RoutingPatterns.SummaryDefaultAgg != "" {
				switch cfg.RoutingPatterns.SummaryDefaultAgg {
				case "min", "max", "sum", "count", "avg", "passthrough":
					// Valid options
				default:
					return fmt.Errorf("invalid summary_default_agg %q, must be one of: min, max, sum, count, avg, passthrough", cfg.RoutingPatterns.SummaryDefaultAgg)
				}
			}
		}
	}

	// Validate max staleness
	if cfg.MaxStaleness < 0 {
		return fmt.Errorf("max_staleness must be non-negative, got %v", cfg.MaxStaleness)
	}

	// Validate include/exclude patterns
	if len(cfg.Include.Patterns) > 0 && cfg.Include.MatchType == "" {
		return fmt.Errorf("include match_type must be specified when patterns are provided")
	}

	if len(cfg.Exclude.Patterns) > 0 && cfg.Exclude.MatchType == "" {
		return fmt.Errorf("exclude match_type must be specified when patterns are provided")
	}

	return nil
}

// GetAggregationTemporality converts string config to pmetric.AggregationTemporality
func (cfg *WindowConfig) GetAggregationTemporality() pmetric.AggregationTemporality {
	switch cfg.AggregationTemporality {
	case "cumulative":
		return pmetric.AggregationTemporalityCumulative
	default:
		// Default to delta for window-based aggregations
		return pmetric.AggregationTemporalityDelta
	}
}
