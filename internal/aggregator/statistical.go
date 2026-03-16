// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregator // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor/internal/aggregator"

import (
	"math"
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor/internal/filter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor/internal/identity"
)

// AvgAccumulator accumulates sum and count for average calculation
type AvgAccumulator struct {
	sum   float64
	count uint64
	ts    pcommon.Timestamp
	mu    sync.Mutex
}

// StatisticalAggregator performs min/max/sum/count/avg aggregations
type StatisticalAggregator struct {
	// Concurrent maps for each aggregation type
	minValues   sync.Map // key: string -> value: *minValue
	maxValues   sync.Map // key: string -> value: *maxValue
	sumValues   sync.Map // key: string -> value: *sumValue
	countValues sync.Map // key: string -> value: *countValue
	avgValues   sync.Map // key: string -> value: *AvgAccumulator

	// Store identities for result emission
	identities sync.Map // key: string -> value: identity.MetricIdentity
	baseNames  sync.Map // key: string -> value: string (base metric name)

	// Store previous cumulative values for delta conversion
	prevCumulativeValues sync.Map // key: string -> value: *prevValue

	// Staleness configuration
	maxStaleness pcommon.Timestamp // Duration in nanoseconds for state cleanup

	// Callbacks for telemetry
	onStateCreated func(aggType filter.AggregationType, metricName string)
	onStateExpired func(count int64, aggType filter.AggregationType, metricName string)
}

// prevValue stores the previous cumulative value for delta conversion
type prevValue struct {
	value float64
	ts    pcommon.Timestamp
	mu    sync.Mutex
}

// minValue wraps a float64 with mutex for min aggregation
type minValue struct {
	value float64
	ts    pcommon.Timestamp
	mu    sync.Mutex
}

// maxValue wraps a float64 with mutex for max aggregation
type maxValue struct {
	value float64
	ts    pcommon.Timestamp
	mu    sync.Mutex
}

// sumValue wraps a float64 with mutex for sum aggregation
type sumValue struct {
	value float64
	ts    pcommon.Timestamp
	mu    sync.Mutex
}

// countValue wraps a uint64 with mutex for count aggregation
type countValue struct {
	value uint64
	ts    pcommon.Timestamp
	mu    sync.Mutex
}

// NewStatisticalAggregator creates a new StatisticalAggregator
func NewStatisticalAggregator(maxStaleness pcommon.Timestamp) *StatisticalAggregator {
	return &StatisticalAggregator{
		maxStaleness: maxStaleness,
	}
}

// SetTelemetryCallbacks sets the callbacks for state creation/expiration telemetry
func (sa *StatisticalAggregator) SetTelemetryCallbacks(
	onStateCreated func(aggType filter.AggregationType, metricName string),
	onStateExpired func(count int64, aggType filter.AggregationType, metricName string),
) {
	sa.onStateCreated = onStateCreated
	sa.onStateExpired = onStateExpired
}

// ConvertCumulativeToDelta converts a cumulative counter value to delta
// For the first datapoint, returns the cumulative value itself as the delta
func (sa *StatisticalAggregator) ConvertCumulativeToDelta(key string, cumulativeValue float64, ts pcommon.Timestamp) float64 {
	actual, loaded := sa.prevCumulativeValues.LoadOrStore(key, &prevValue{value: cumulativeValue, ts: ts})
	if !loaded {
		// First datapoint - use the cumulative value as the first delta
		// This ensures we don't lose data
		return cumulativeValue
	}

	pv := actual.(*prevValue)
	pv.mu.Lock()
	defer pv.mu.Unlock()

	// Calculate delta
	delta := cumulativeValue - pv.value

	// Handle counter resets (when cumulative value decreases)
	if delta < 0 {
		// Counter reset detected - use the new cumulative value as the delta
		delta = cumulativeValue
	}

	// Update stored value for next comparison
	pv.value = cumulativeValue
	pv.ts = ts

	return delta
}

// Aggregate adds a value to the appropriate aggregation
func (sa *StatisticalAggregator) Aggregate(
	key string,
	aggType filter.AggregationType,
	value float64,
	ident identity.MetricIdentity,
	baseName string,
	timestamp pcommon.Timestamp,
) {
	// Store identity and base name only if not already present
	// This optimization reduces heap allocations by ~90% in steady-state
	if _, exists := sa.identities.Load(key); !exists {
		sa.identities.Store(key, ident.Clone())
		sa.baseNames.Store(key, baseName)
	}

	switch aggType {
	case filter.AggMin:
		sa.aggregateMin(key, value, timestamp, baseName)
	case filter.AggMax:
		sa.aggregateMax(key, value, timestamp, baseName)
	case filter.AggSum:
		sa.aggregateSum(key, value, timestamp, baseName)
	case filter.AggCount:
		sa.aggregateCount(key, value, timestamp, baseName)
	case filter.AggAvg:
		sa.aggregateAvg(key, value, timestamp, baseName)
	}
}

func (sa *StatisticalAggregator) aggregateMin(key string, value float64, ts pcommon.Timestamp, baseName string) {
	actual, loaded := sa.minValues.LoadOrStore(key, &minValue{value: value, ts: ts})
	if !loaded {
		// New state created
		if sa.onStateCreated != nil {
			sa.onStateCreated(filter.AggMin, baseName)
		}
		return
	}

	mv := actual.(*minValue)
	mv.mu.Lock()
	defer mv.mu.Unlock()

	if value < mv.value {
		mv.value = value
		mv.ts = ts
	}
}

func (sa *StatisticalAggregator) aggregateMax(key string, value float64, ts pcommon.Timestamp, baseName string) {
	actual, loaded := sa.maxValues.LoadOrStore(key, &maxValue{value: value, ts: ts})
	if !loaded {
		// New state created
		if sa.onStateCreated != nil {
			sa.onStateCreated(filter.AggMax, baseName)
		}
		return
	}

	mv := actual.(*maxValue)
	mv.mu.Lock()
	defer mv.mu.Unlock()

	if value > mv.value {
		mv.value = value
		mv.ts = ts
	}
}

func (sa *StatisticalAggregator) aggregateSum(key string, value float64, ts pcommon.Timestamp, baseName string) {
	actual, loaded := sa.sumValues.LoadOrStore(key, &sumValue{value: value, ts: ts})
	if !loaded {
		// New state created
		if sa.onStateCreated != nil {
			sa.onStateCreated(filter.AggSum, baseName)
		}
		return
	}

	sv := actual.(*sumValue)
	sv.mu.Lock()
	defer sv.mu.Unlock()

	sv.value += value
	if ts > sv.ts {
		sv.ts = ts
	}
}

func (sa *StatisticalAggregator) aggregateCount(key string, value float64, ts pcommon.Timestamp, baseName string) {
	actual, loaded := sa.countValues.LoadOrStore(key, &countValue{value: 1, ts: ts})
	if !loaded {
		// New state created
		if sa.onStateCreated != nil {
			sa.onStateCreated(filter.AggCount, baseName)
		}
		return
	}

	cv := actual.(*countValue)
	cv.mu.Lock()
	defer cv.mu.Unlock()

	// COUNT aggregation: increment by 1 for each datapoint (not by value!)
	cv.value++
	if ts > cv.ts {
		cv.ts = ts
	}
}

func (sa *StatisticalAggregator) aggregateAvg(key string, value float64, ts pcommon.Timestamp, baseName string) {
	actual, loaded := sa.avgValues.LoadOrStore(key, &AvgAccumulator{sum: value, count: 1, ts: ts})
	if !loaded {
		// New state created
		if sa.onStateCreated != nil {
			sa.onStateCreated(filter.AggAvg, baseName)
		}
		return
	}

	acc := actual.(*AvgAccumulator)
	acc.mu.Lock()
	defer acc.mu.Unlock()

	acc.sum += value
	acc.count++
	// Update timestamp to the latest
	if ts > acc.ts {
		acc.ts = ts
	}
}

// Flush extracts all aggregated metrics and resets the state
func (sa *StatisticalAggregator) Flush(latestTimestamp pcommon.Timestamp) []AggregatedMetric {
	results := make([]AggregatedMetric, 0)

	// Collect min values
	sa.minValues.Range(func(k, v interface{}) bool {
		key := k.(string)
		mv := v.(*minValue)

		mv.mu.Lock()
		value := mv.value
		ts := mv.ts
		mv.mu.Unlock()

		if ident, ok := sa.identities.Load(key); ok {
			baseName, _ := sa.baseNames.Load(key)
			results = append(results, AggregatedMetric{
				Identity:  ident.(identity.MetricIdentity),
				AggType:   filter.AggMin,
				Value:     value,
				Timestamp: ts,
				BaseName:  baseName.(string),
			})
		}
		return true
	})

	// Collect max values
	sa.maxValues.Range(func(k, v interface{}) bool {
		key := k.(string)
		mv := v.(*maxValue)

		mv.mu.Lock()
		value := mv.value
		ts := mv.ts
		mv.mu.Unlock()

		if ident, ok := sa.identities.Load(key); ok {
			baseName, _ := sa.baseNames.Load(key)
			results = append(results, AggregatedMetric{
				Identity:  ident.(identity.MetricIdentity),
				AggType:   filter.AggMax,
				Value:     value,
				Timestamp: ts,
				BaseName:  baseName.(string),
			})
		}
		return true
	})

	// Collect sum values
	sa.sumValues.Range(func(k, v interface{}) bool {
		key := k.(string)
		sv := v.(*sumValue)

		sv.mu.Lock()
		value := sv.value
		ts := sv.ts
		sv.mu.Unlock()

		if ident, ok := sa.identities.Load(key); ok {
			baseName, _ := sa.baseNames.Load(key)
			results = append(results, AggregatedMetric{
				Identity:  ident.(identity.MetricIdentity),
				AggType:   filter.AggSum,
				Value:     value,
				Timestamp: ts,
				BaseName:  baseName.(string),
			})
		}
		return true
	})

	// Collect count values
	sa.countValues.Range(func(k, v interface{}) bool {
		key := k.(string)
		cv := v.(*countValue)

		cv.mu.Lock()
		value := cv.value
		ts := cv.ts
		cv.mu.Unlock()

		if ident, ok := sa.identities.Load(key); ok {
			baseName, _ := sa.baseNames.Load(key)
			results = append(results, AggregatedMetric{
				Identity:  ident.(identity.MetricIdentity),
				AggType:   filter.AggCount,
				Value:     float64(value),
				Timestamp: ts,
				BaseName:  baseName.(string),
			})
		}
		return true
	})

	// Collect avg values
	sa.avgValues.Range(func(k, v interface{}) bool {
		key := k.(string)
		acc := v.(*AvgAccumulator)

		acc.mu.Lock()
		sum := acc.sum
		count := acc.count
		ts := acc.ts
		acc.mu.Unlock()

		if ident, ok := sa.identities.Load(key); ok {
			baseName, _ := sa.baseNames.Load(key)
			avg := sum / float64(count)
			if math.IsNaN(avg) || math.IsInf(avg, 0) {
				avg = 0
			}
			results = append(results, AggregatedMetric{
				Identity:  ident.(identity.MetricIdentity),
				AggType:   filter.AggAvg,
				Value:     avg,
				Timestamp: ts,
				BaseName:  baseName.(string),
			})
		}
		return true
	})

	// Count states before expiring them (for telemetry)
	if sa.onStateExpired != nil {
		metricCounts := sa.GetStateCountsByMetric()
		// Report expiration counts per metric and aggregation type
		for metricName, aggCounts := range metricCounts {
			for aggType, count := range aggCounts {
				if count > 0 {
					sa.onStateExpired(count, aggType, metricName)
				}
			}
		}
	}

	// Reset all maps except prevCumulativeValues
	// prevCumulativeValues must persist across windows for correct delta calculation
	sa.minValues = sync.Map{}
	sa.maxValues = sync.Map{}
	sa.sumValues = sync.Map{}
	sa.countValues = sync.Map{}
	sa.avgValues = sync.Map{}
	sa.identities = sync.Map{}
	sa.baseNames = sync.Map{}

	// Clean up stale cumulative tracking states
	if sa.maxStaleness > 0 {
		sa.cleanupStaleCumulativeStates(latestTimestamp)
	}

	return results
}

// StateCounts holds the count of states by aggregation type
type StateCounts struct {
	Min   int64
	Max   int64
	Sum   int64
	Count int64
	Avg   int64
}

// MetricStateCounts holds state counts per metric name
type MetricStateCounts map[string]map[filter.AggregationType]int64

// GetStateCounts returns the current count of active states by aggregation type
func (sa *StatisticalAggregator) GetStateCounts() StateCounts {
	counts := StateCounts{}

	// Count active states in each map
	sa.minValues.Range(func(k, v interface{}) bool {
		counts.Min++
		return true
	})

	sa.maxValues.Range(func(k, v interface{}) bool {
		counts.Max++
		return true
	})

	sa.sumValues.Range(func(k, v interface{}) bool {
		counts.Sum++
		return true
	})

	sa.countValues.Range(func(k, v interface{}) bool {
		counts.Count++
		return true
	})

	sa.avgValues.Range(func(k, v interface{}) bool {
		counts.Avg++
		return true
	})

	return counts
}

// GetStateCountsByMetric returns state counts broken down by metric name and aggregation type
func (sa *StatisticalAggregator) GetStateCountsByMetric() MetricStateCounts {
	counts := make(MetricStateCounts)

	// Helper to count and track by metric name
	countByMetric := func(aggType filter.AggregationType, stateMap *sync.Map) {
		stateMap.Range(func(k, v interface{}) bool {
			key := k.(string)
			// Get the base name for this key
			if baseNameVal, ok := sa.baseNames.Load(key); ok {
				baseName := baseNameVal.(string)
				if counts[baseName] == nil {
					counts[baseName] = make(map[filter.AggregationType]int64)
				}
				counts[baseName][aggType]++
			}
			return true
		})
	}

	// Count states for each aggregation type
	countByMetric(filter.AggMin, &sa.minValues)
	countByMetric(filter.AggMax, &sa.maxValues)
	countByMetric(filter.AggSum, &sa.sumValues)
	countByMetric(filter.AggCount, &sa.countValues)
	countByMetric(filter.AggAvg, &sa.avgValues)

	return counts
}

// GetCumulativeStateCount returns the current count of cumulative tracking states
func (sa *StatisticalAggregator) GetCumulativeStateCount() int64 {
	count := int64(0)
	sa.prevCumulativeValues.Range(func(k, v interface{}) bool {
		count++
		return true
	})
	return count
}

// cleanupStaleCumulativeStates removes cumulative tracking states that haven't been updated
// within the maxStaleness duration
func (sa *StatisticalAggregator) cleanupStaleCumulativeStates(currentTimestamp pcommon.Timestamp) {
	keysToDelete := []string{}

	// Identify stale entries
	sa.prevCumulativeValues.Range(func(k, v interface{}) bool {
		key := k.(string)
		pv := v.(*prevValue)

		pv.mu.Lock()
		age := currentTimestamp - pv.ts
		pv.mu.Unlock()

		// If the state hasn't been updated for longer than maxStaleness, mark for deletion
		if age > sa.maxStaleness {
			keysToDelete = append(keysToDelete, key)
		}

		return true
	})

	// Delete stale entries
	for _, key := range keysToDelete {
		sa.prevCumulativeValues.Delete(key)
	}
}
