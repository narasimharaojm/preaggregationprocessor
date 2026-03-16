// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package preaggregationprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor"

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/preaggregationprocessor/internal/metadata"
)

// NewFactory creates a new processor factory
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, metadata.MetricsStability),
	)
}

// createDefaultConfig creates the default configuration for the processor
func createDefaultConfig() component.Config {
	return &Config{
		Mode: ModeStatistical,
		Window: WindowConfig{
			Duration:               60 * time.Second,
			AggregationTemporality: "delta",
		},
		RoutingPatterns: RoutingConfig{
			MatchType:         MatchTypeGlob, // Default to glob for user-friendliness
			EnableTypeBased:   false,
			GaugeDefaultAgg:   "avg",
			CounterDefaultAgg: "sum",
		},
		MaxStaleness: 1 * time.Hour,
	}
}

// createMetricsProcessor creates a metrics processor based on this config
func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	processorConfig, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("configuration parsing error")
	}

	metricsProcessor, err := newProcessor(processorConfig, set, nextConsumer)
	if err != nil {
		return nil, err
	}

	return processorhelper.NewMetrics(
		ctx,
		set,
		cfg,
		nextConsumer,
		metricsProcessor.ConsumeMetrics,
		processorhelper.WithCapabilities(processorCapabilities),
		processorhelper.WithStart(metricsProcessor.Start),
		processorhelper.WithShutdown(metricsProcessor.Shutdown),
	)
}

var processorCapabilities = consumer.Capabilities{MutatesData: true}
