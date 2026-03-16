// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package identity // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor/internal/identity"

import (
	"bytes"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
)

const (
	SEP = byte(0x1E) // ASCII Record Separator
)

// MetricIdentity represents a unique identity for a metric based on its resource,
// scope, metric name, unit, and datapoint attributes
type MetricIdentity struct {
	Resource   pcommon.Resource
	Scope      pcommon.InstrumentationScope
	MetricName string
	MetricUnit string
	Attributes pcommon.Map // Datapoint attributes
}

// Key generates a unique string key for this metric identity.
// The key combines resource attributes, metric name, and datapoint attributes
// using consistent hashing for deterministic grouping.
func (mi *MetricIdentity) Key() string {
	// Pre-allocate buffer with estimated capacity to avoid reallocations
	// Typical key size: 16 (resHash) + 1 + scopeName + 1 + scopeVer + 1 + metricName + 1 + unit + 1 + 16 (attrHash)
	// Estimate ~128 bytes to handle most cases without reallocation
	buf := bytes.NewBuffer(make([]byte, 0, 128))

	// Write resource attributes hash if present
	if mi.Resource.Attributes().Len() > 0 {
		resHash := pdatautil.MapHash(mi.Resource.Attributes())
		buf.Write(resHash[:])
	}
	buf.WriteByte(SEP)

	// Write scope name and version
	buf.WriteString(mi.Scope.Name())
	buf.WriteByte(SEP)
	buf.WriteString(mi.Scope.Version())
	buf.WriteByte(SEP)

	// Write metric name
	buf.WriteString(mi.MetricName)
	buf.WriteByte(SEP)

	// Write metric unit
	buf.WriteString(mi.MetricUnit)
	buf.WriteByte(SEP)

	// Write datapoint attributes hash if present
	if mi.Attributes.Len() > 0 {
		attrsHash := pdatautil.MapHash(mi.Attributes)
		buf.Write(attrsHash[:])
	}

	return buf.String()
}

// Clone creates a deep copy of the MetricIdentity
func (mi *MetricIdentity) Clone() MetricIdentity {
	clone := MetricIdentity{
		Resource:   pcommon.NewResource(),
		Scope:      pcommon.NewInstrumentationScope(),
		MetricName: mi.MetricName,
		MetricUnit: mi.MetricUnit,
		Attributes: pcommon.NewMap(),
	}

	mi.Resource.CopyTo(clone.Resource)
	mi.Scope.CopyTo(clone.Scope)
	mi.Attributes.CopyTo(clone.Attributes)

	return clone
}
