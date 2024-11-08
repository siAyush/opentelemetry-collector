package clickhouseexporter

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	typeStr   = "clickhouse"
	stability = component.StabilityLevelBeta
)

// NewFactory creates a factory for ClickHouse exporter
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType("clickhouse"),
		createDefaultConfig,
		exporter.WithMetrics(createMetricExporter, stability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		MetricsTableName: "metricsTable",
	}
}

func createMetricExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	c := cfg.(*Config)
	exporter, err := newMetricsExporter(c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure clickhouse metrics exporter: %w", err)
	}

	return exporterhelper.NewMetrics(
		ctx,
		set,
		cfg,
		exporter.ConsumeMetrics,
		exporterhelper.WithStart(exporter.start),
		exporterhelper.WithShutdown(exporter.shutdown),
	)
}
