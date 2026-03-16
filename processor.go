// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package preaggregationprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor"

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/filterset"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor/internal/aggregator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor/internal/emitter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor/internal/filter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor/internal/identity"
)

type preAggregationProcessor struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger *zap.Logger

	config       *Config
	nextConsumer consumer.Metrics

	// State lock for flush/consume coordination
	stateLock sync.Mutex

	// Filtering
	filter *filter.Filter

	// Aggregators
	statisticalAgg *aggregator.StatisticalAggregator

	// Emitter
	emitter *emitter.Emitter

	// Window timing
	windowStart pcommon.Timestamp

	// Telemetry
	telemetry *telemetry

	// Metrics for cardinality tracking (per window)
	inputDatapoints       int64
	outputDatapoints      int64
	cardinalityReduction  int64  // Snapshot of reduction from last flush
}

func newProcessor(config *Config, set processor.Settings, nextConsumer consumer.Metrics) (*preAggregationProcessor, error) {
	ctx, cancel := context.WithCancel(context.Background())
	logger := set.Logger

	// Create include/exclude filter sets
	var includeFS filterset.FilterSet
	var excludeFS filterset.FilterSet
	var err error

	if len(config.Include.Patterns) > 0 {
		includeConfig := config.Include.Config
		if config.Include.RegexpConfig != nil {
			includeConfig.RegexpConfig = config.Include.RegexpConfig
		}

		includePatterns := config.Include.Patterns
		// Convert glob to regex if include uses glob match type
		if config.Include.MatchType == MatchTypeGlob {
			includePatterns, err = convertGlobPatternsToRegex(includePatterns)
			if err != nil {
				cancel()
				return nil, err
			}
			// Set effective match type to regexp after conversion
			includeConfig.MatchType = filterset.Regexp
		}

		includeFS, err = filterset.CreateFilterSet(includePatterns, &includeConfig)
		if err != nil {
			cancel()
			return nil, err
		}
	}

	if len(config.Exclude.Patterns) > 0 {
		excludeConfig := config.Exclude.Config
		if config.Exclude.RegexpConfig != nil {
			excludeConfig.RegexpConfig = config.Exclude.RegexpConfig
		}

		excludePatterns := config.Exclude.Patterns
		// Convert glob to regex if exclude uses glob match type
		if config.Exclude.MatchType == MatchTypeGlob {
			excludePatterns, err = convertGlobPatternsToRegex(excludePatterns)
			if err != nil {
				cancel()
				return nil, err
			}
			// Set effective match type to regexp after conversion
			excludeConfig.MatchType = filterset.Regexp
		}

		excludeFS, err = filterset.CreateFilterSet(excludePatterns, &excludeConfig)
		if err != nil {
			cancel()
			return nil, err
		}
	}

	// Create pattern filtersets for each aggregation type
	var minPatternsFS, maxPatternsFS, sumPatternsFS, countPatternsFS, avgPatternsFS filterset.FilterSet

	// Create base filterset config with user-specified match type
	// Default to Glob if not specified (user-friendly)
	matchType := config.RoutingPatterns.MatchType
	if matchType == "" {
		matchType = MatchTypeGlob
	}

	// If using glob mode, we'll convert patterns to regex and use Regexp matching
	effectiveMatchType := matchType
	if matchType == MatchTypeGlob {
		effectiveMatchType = filterset.Regexp
	}

	routingConfig := &filterset.Config{
		MatchType: effectiveMatchType,
	}

	// Only add regex config if using regexp match type (or converted from glob)
	if effectiveMatchType == filterset.Regexp && config.RoutingPatterns.RegexpConfig != nil {
		routingConfig.RegexpConfig = config.RoutingPatterns.RegexpConfig
	}

	// Convert glob patterns to regex if needed, then create filtersets
	if len(config.RoutingPatterns.MinPatterns) > 0 {
		minPatterns := config.RoutingPatterns.MinPatterns
		if matchType == MatchTypeGlob {
			minPatterns, err = convertGlobPatternsToRegex(minPatterns)
			if err != nil {
				cancel()
				return nil, err
			}
		}
		minPatternsFS, err = filterset.CreateFilterSet(minPatterns, routingConfig)
		if err != nil {
			cancel()
			return nil, err
		}
	}

	if len(config.RoutingPatterns.MaxPatterns) > 0 {
		maxPatterns := config.RoutingPatterns.MaxPatterns
		if matchType == MatchTypeGlob {
			maxPatterns, err = convertGlobPatternsToRegex(maxPatterns)
			if err != nil {
				cancel()
				return nil, err
			}
		}
		maxPatternsFS, err = filterset.CreateFilterSet(maxPatterns, routingConfig)
		if err != nil {
			cancel()
			return nil, err
		}
	}

	if len(config.RoutingPatterns.SumPatterns) > 0 {
		sumPatterns := config.RoutingPatterns.SumPatterns
		if matchType == MatchTypeGlob {
			sumPatterns, err = convertGlobPatternsToRegex(sumPatterns)
			if err != nil {
				cancel()
				return nil, err
			}
		}
		sumPatternsFS, err = filterset.CreateFilterSet(sumPatterns, routingConfig)
		if err != nil {
			cancel()
			return nil, err
		}
	}

	if len(config.RoutingPatterns.CountPatterns) > 0 {
		countPatterns := config.RoutingPatterns.CountPatterns
		if matchType == MatchTypeGlob {
			countPatterns, err = convertGlobPatternsToRegex(countPatterns)
			if err != nil {
				cancel()
				return nil, err
			}
		}
		countPatternsFS, err = filterset.CreateFilterSet(countPatterns, routingConfig)
		if err != nil {
			cancel()
			return nil, err
		}
	}

	if len(config.RoutingPatterns.AvgPatterns) > 0 {
		avgPatterns := config.RoutingPatterns.AvgPatterns
		if matchType == MatchTypeGlob {
			avgPatterns, err = convertGlobPatternsToRegex(avgPatterns)
			if err != nil {
				cancel()
				return nil, err
			}
		}
		avgPatternsFS, err = filterset.CreateFilterSet(avgPatterns, routingConfig)
		if err != nil {
			cancel()
			return nil, err
		}
	}

	// Convert string aggregation types to filter.AggregationType
	gaugeDefaultAgg := stringToAggType(config.RoutingPatterns.GaugeDefaultAgg)
	histogramDefaultAgg := stringToAggType(config.RoutingPatterns.HistogramDefaultAgg)
	summaryDefaultAgg := stringToAggType(config.RoutingPatterns.SummaryDefaultAgg)

	// Create filter with routing patterns
	routingPatterns := filter.RoutingPatterns{
		MinPatterns:   minPatternsFS,
		MaxPatterns:   maxPatternsFS,
		SumPatterns:   sumPatternsFS,
		CountPatterns: countPatternsFS,
		AvgPatterns:   avgPatternsFS,

		EnableTypeBased:     config.RoutingPatterns.EnableTypeBased,
		GaugeDefaultAgg:     gaugeDefaultAgg,
		CounterDefaultAgg:   config.RoutingPatterns.CounterDefaultAgg,
		HistogramDefaultAgg: histogramDefaultAgg,
		SummaryDefaultAgg:   summaryDefaultAgg,
	}

	metricFilter := filter.NewFilter(includeFS, excludeFS, routingPatterns)

	// Create aggregators
	// Convert MaxStaleness duration to pcommon.Timestamp (nanoseconds)
	maxStalenessNanos := pcommon.Timestamp(config.MaxStaleness.Nanoseconds())
	statAgg := aggregator.NewStatisticalAggregator(maxStalenessNanos)

	// Create emitter
	emit := emitter.NewEmitter(config.Window.GetAggregationTemporality())

	// Create processor instance
	p := &preAggregationProcessor{
		ctx:            ctx,
		cancel:         cancel,
		logger:         logger,
		config:         config,
		nextConsumer:   nextConsumer,
		filter:         metricFilter,
		statisticalAgg: statAgg,
		emitter:        emit,
		windowStart:    pcommon.NewTimestampFromTime(time.Now()),
	}

	// Initialize telemetry
	meter := set.MeterProvider.Meter(meterName)
	tel, err := newTelemetry(meter, p)
	if err != nil {
		cancel()
		return nil, err
	}
	p.telemetry = tel

	// Register telemetry callbacks for state lifecycle tracking
	if tel != nil {
		statAgg.SetTelemetryCallbacks(
			func(aggType filter.AggregationType, metricName string) {
				// State created callback
				p.telemetry.statesCreated.Add(p.ctx, 1, metric.WithAttributes(
					attribute.String("agg_type", aggType.String()),
					attribute.String("metric_name", metricName),
				))
			},
			func(count int64, aggType filter.AggregationType, metricName string) {
				// States expired callback
				p.telemetry.statesExpired.Add(p.ctx, count, metric.WithAttributes(
					attribute.String("agg_type", aggType.String()),
					attribute.String("metric_name", metricName),
				))
			},
		)
	}

	return p, nil
}

func (p *preAggregationProcessor) Start(_ context.Context, _ component.Host) error {
	// Calculate next wall-clock aligned boundary
	now := time.Now()
	windowDuration := p.config.Window.Duration

	nextBoundary := now.Truncate(windowDuration).Add(windowDuration)
	initialDelay := nextBoundary.Sub(now)

	p.logger.Info("Pre-aggregation processor started",
		zap.String("mode", string(p.config.Mode)),
		zap.Duration("window_duration", windowDuration),
		zap.Time("current_time", now),
		zap.Time("next_boundary", nextBoundary),
		zap.Duration("initial_delay", initialDelay),
	)

	// Start window flush with aligned boundaries
	go func() {
		timer := time.NewTimer(initialDelay)
		select {
		case <-p.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			p.flushWindow()
		}

		ticker := time.NewTicker(windowDuration)
		defer ticker.Stop()

		for {
			select {
			case <-p.ctx.Done():
				return
			case <-ticker.C:
				p.flushWindow()
			}
		}
	}()

	return nil
}

func (p *preAggregationProcessor) Shutdown(_ context.Context) error {
	p.cancel()
	p.logger.Info("Pre-aggregation processor shutdown")
	return nil
}

func (p *preAggregationProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	startTime := time.Now()
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	// Count incoming metrics and datapoints
	metricCount := int64(md.MetricCount())
	datapointCount := int64(md.DataPointCount())

	if p.telemetry != nil {
		p.telemetry.metricsReceived.Add(ctx, metricCount)
		p.telemetry.datapointsProcessed.Add(ctx, datapointCount)
	}
	p.inputDatapoints += datapointCount

	metricsAggregated := int64(0)
	metricsPassthrough := int64(0)

	// Process metrics using RemoveIf pattern
	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			sm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				metricName := m.Name()

				// Apply filtering
				if !p.filter.ShouldProcess(metricName) {
					metricsPassthrough++
					return false // Keep (pass through)
				}

				// Priority 1: Try pattern-based routing first (highest priority)
				aggType, baseName := p.filter.DetectAggType(metricName)
				patternMatched := aggType != filter.AggUnknown

				if patternMatched && p.telemetry != nil {
					p.telemetry.patternMatches.Add(ctx, 1, metric.WithAttributes(attribute.String("agg_type", aggType.String())))
				}

				// If pattern matched, use that aggregation type exclusively
				if patternMatched {
					p.processStatistical(ctx, rm, sm, m, aggType, baseName)
					metricsAggregated++
					return true // Remove from batch
				}

				// Priority 2: Try type-based routing for Sum (counter) metrics
				if m.Type() == pmetric.MetricTypeSum {
					counterAggTypes := p.filter.GetCounterAggTypes()
					if len(counterAggTypes) > 0 {
						if p.telemetry != nil {
							p.telemetry.typeBasedRoute.Add(ctx, 1, metric.WithAttributes(attribute.String("metric_type", "sum")))
						}
						for _, caggType := range counterAggTypes {
							p.processStatistical(ctx, rm, sm, m, caggType, metricName)
						}
						metricsAggregated++
						return true // Remove from batch
					}
				}

				// Priority 3: Try type-based routing for Gauge metrics
				if m.Type() == pmetric.MetricTypeGauge {
					gaugeAggType := p.filter.GetGaugeDefaultAgg()
					if gaugeAggType != filter.AggUnknown {
						if p.telemetry != nil {
							p.telemetry.typeBasedRoute.Add(ctx, 1, metric.WithAttributes(attribute.String("metric_type", "gauge")))
						}
						p.processStatistical(ctx, rm, sm, m, gaugeAggType, metricName)
						metricsAggregated++
						return true // Remove from batch
					}
				}

				// Pattern miss - no routing matched
				if !patternMatched && p.telemetry != nil {
					p.telemetry.patternMisses.Add(ctx, 1)
				}

				metricsPassthrough++
				return false // Keep (pass through)
			})
			return sm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})

	// Record telemetry
	if p.telemetry != nil {
		p.telemetry.metricsAggregated.Add(ctx, metricsAggregated)
		p.telemetry.metricsPassthrough.Add(ctx, metricsPassthrough)
		duration := time.Since(startTime).Seconds()
		p.telemetry.processingDuration.Record(ctx, duration)
	}

	return md, nil
}

func (p *preAggregationProcessor) processStatistical(
	ctx context.Context,
	rm pmetric.ResourceMetrics,
	sm pmetric.ScopeMetrics,
	m pmetric.Metric,
	aggType filter.AggregationType,
	baseName string,
) {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			value := getNumberDataPointValue(dp)
			timestamp := dp.Timestamp()

			ident := identity.MetricIdentity{
				Resource:   rm.Resource(),
				Scope:      sm.Scope(),
				MetricName: baseName,
				MetricUnit: m.Unit(),
				Attributes: dp.Attributes(),
			}

			key := ident.Key()
			p.statisticalAgg.Aggregate(key, aggType, value, ident, baseName, timestamp)
		}

	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		isCumulative := m.Sum().AggregationTemporality() == pmetric.AggregationTemporalityCumulative

		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			value := getNumberDataPointValue(dp)
			timestamp := dp.Timestamp()

			ident := identity.MetricIdentity{
				Resource:   rm.Resource(),
				Scope:      sm.Scope(),
				MetricName: baseName,
				MetricUnit: m.Unit(),
				Attributes: dp.Attributes(),
			}

			key := ident.Key()

			if isCumulative {
				deltaValue := p.statisticalAgg.ConvertCumulativeToDelta(key, value, timestamp)
				if p.telemetry != nil {
					p.telemetry.cumulativeToDelta.Add(ctx, 1)
				}
				if deltaValue > 0 {
					p.statisticalAgg.Aggregate(key, aggType, deltaValue, ident, baseName, timestamp)
				} else if p.telemetry != nil {
					p.telemetry.deltaValuesSkipped.Add(ctx, 1)
				}
			} else {
				p.statisticalAgg.Aggregate(key, aggType, value, ident, baseName, timestamp)
			}
		}
	}
}

func (p *preAggregationProcessor) flushWindow() {
	startTime := time.Now()
	p.stateLock.Lock()
	defer p.stateLock.Unlock()

	windowEnd := pcommon.NewTimestampFromTime(time.Now())
	windowStart := p.windowStart

	totalMetricsEmitted := int64(0)
	totalDatapointsEmitted := int64(0)

	// Flush statistical aggregations
	aggregated := p.statisticalAgg.Flush(windowEnd)
	if len(aggregated) > 0 {
		md := p.emitter.EmitStatistical(aggregated, windowStart, windowEnd)
		if md.MetricCount() > 0 {
			metricsEmitted := int64(md.MetricCount())
			datapointsEmitted := int64(md.DataPointCount())
			totalMetricsEmitted += metricsEmitted
			totalDatapointsEmitted += datapointsEmitted

			if err := p.nextConsumer.ConsumeMetrics(p.ctx, md); err != nil {
				p.logger.Error("Failed to emit statistical metrics", zap.Error(err), zap.Int("metric_count", md.MetricCount()))
				if p.telemetry != nil {
					p.telemetry.emissionFailures.Add(p.ctx, 1)
				}
			} else {
				p.logger.Debug("Emitted statistical metrics", zap.Int("metric_count", md.MetricCount()))
				if p.telemetry != nil {
					p.telemetry.metricsEmitted.Add(p.ctx, metricsEmitted)
				}
			}
		}
	}

	// Calculate and snapshot cardinality reduction for this window
	p.cardinalityReduction = p.inputDatapoints - totalDatapointsEmitted

	// Update output datapoints for reference
	p.outputDatapoints = totalDatapointsEmitted

	// Record window flush telemetry
	if p.telemetry != nil {
		p.telemetry.windowFlushes.Add(p.ctx, 1)
		flushDuration := time.Since(startTime).Seconds()
		p.telemetry.windowFlushDuration.Record(p.ctx, flushDuration)
		p.telemetry.windowMetricsCount.Record(p.ctx, totalMetricsEmitted)
	}

	p.windowStart = windowEnd

	// Reset input datapoints for next window
	p.inputDatapoints = 0
}

func getNumberDataPointValue(dp pmetric.NumberDataPoint) float64 {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		return dp.DoubleValue()
	case pmetric.NumberDataPointValueTypeInt:
		return float64(dp.IntValue())
	default:
		return 0
	}
}

func stringToAggType(s string) filter.AggregationType {
	switch s {
	case "min":
		return filter.AggMin
	case "max":
		return filter.AggMax
	case "sum":
		return filter.AggSum
	case "count":
		return filter.AggCount
	case "avg":
		return filter.AggAvg
	default:
		return filter.AggUnknown
	}
}

func (p *preAggregationProcessor) getCardinalityReduction() int64 {
	p.stateLock.Lock()
	defer p.stateLock.Unlock()
	// Return the snapshot from last flush
	return p.cardinalityReduction
}
