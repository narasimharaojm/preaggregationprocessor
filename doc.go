// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// Package preaggregationprocessor implements a processor that performs metric pre-aggregation.
// It supports two modes: statistical aggregation (min/max/sum/count/avg) and histogram reconstruction
// from component streams. The processor aggregates metrics within configurable time windows and
// emits aggregated results at configured intervals.
package preaggregationprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor"
