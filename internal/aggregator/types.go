// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregator // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor/internal/aggregator"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor/internal/filter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor/internal/identity"
)

// AggregatedMetric represents a metric after aggregation
type AggregatedMetric struct {
	Identity   identity.MetricIdentity
	AggType    filter.AggregationType
	Value      float64
	Timestamp  pcommon.Timestamp
	BaseName   string // Base metric name without suffix
}
