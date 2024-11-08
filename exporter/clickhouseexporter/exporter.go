package clickhouseexporter

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type clickhouseExporter struct {
	db     *sql.DB
	config *Config
}

// Schema for metrics storage (no exemplars now)
const createTableSQL = `
CREATE TABLE IF NOT EXISTS metrics (
    timestamp DateTime64(9),
    name LowCardinality(String),
    description String,
    unit String,
    metric_type Enum8('gauge' = 1, 'sum' = 2, 'histogram' = 3),
    value Float64,
    attributes Map(String, String),
    resource_attributes Map(String, String)
) ENGINE = MergeTree()
ORDER BY (timestamp, name)
PARTITION BY toYYYYMM(timestamp)
SETTINGS index_granularity = 8192
`

// Insert SQL (no exemplars now)
const insertSQL = `
INSERT INTO metrics (
    timestamp, 
    name, 
    description, 
    unit, 
    metric_type, 
    value, 
    attributes, 
    resource_attributes
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
`

// newExporter creates a new ClickHouse exporter
func newMetricsExporter(cfg *Config) (*clickhouseExporter, error) {
	// Configure ClickHouse connection
	dsn, _ := cfg.buildDSN()
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse connection: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return &clickhouseExporter{
		db:     db,
		config: cfg,
	}, nil
}

// start initializes the exporter
func (e *clickhouseExporter) start(ctx context.Context, host component.Host) error {
	// Create tables if they don't exist
	if _, err := e.db.ExecContext(ctx, createTableSQL); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}
	return nil
}

// ConsumeMetrics receives metrics data and exports it to ClickHouse
func (e *clickhouseExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	// Begin transaction
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Prepare statement
	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Process all resource metrics
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		resourceAttrs := attributesToMap(rm.Resource().Attributes())

		// Process all scope metrics
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			metrics := sm.Metrics()

			// Process each metric
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				if err := e.processMetric(ctx, stmt, metric, resourceAttrs); err != nil {
					return err
				}
			}
		}
	}

	return tx.Commit()
}

// processMetric handles different metric types and exports them
func (e *clickhouseExporter) processMetric(ctx context.Context, stmt *sql.Stmt, metric pmetric.Metric, resourceAttrs map[string]string) error {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		return e.processGauge(ctx, stmt, metric, resourceAttrs)
	case pmetric.MetricTypeSum:
		return e.processSum(ctx, stmt, metric, resourceAttrs)
	default:
		return fmt.Errorf("unsupported metric type: %s", metric.Type().String())
	}
}

func (e *clickhouseExporter) processGauge(ctx context.Context, stmt *sql.Stmt, metric pmetric.Metric, resourceAttrs map[string]string) error {
	dps := metric.Gauge().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		if err := e.insertDataPoint(ctx, stmt, "gauge", metric, dp, resourceAttrs); err != nil {
			return err
		}
	}
	return nil
}

func (e *clickhouseExporter) processSum(ctx context.Context, stmt *sql.Stmt, metric pmetric.Metric, resourceAttrs map[string]string) error {
	dps := metric.Sum().DataPoints()
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		if err := e.insertDataPoint(ctx, stmt, "sum", metric, dp, resourceAttrs); err != nil {
			return err
		}
	}
	return nil
}

// insertDataPoint inserts a single data point into the ClickHouse table
func (e *clickhouseExporter) insertDataPoint(
	ctx context.Context,
	stmt *sql.Stmt,
	metricType string,
	metric pmetric.Metric,
	dp pmetric.NumberDataPoint,
	resourceAttrs map[string]string,
) error {
	attributes := attributesToMap(dp.Attributes())
	timestamp := dp.Timestamp().AsTime()
	var value float64

	// Handle the different value types of the data point
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		value = dp.DoubleValue()
	case pmetric.NumberDataPointValueTypeInt:
		value = float64(dp.IntValue())
	}

	// Insert data point into the table
	_, err := stmt.ExecContext(ctx,
		timestamp,
		metric.Name(),
		metric.Description(),
		metric.Unit(),
		metricType,
		value,
		attributes,
		resourceAttrs,
	)
	return err
}

// attributesToMap converts OpenTelemetry attributes to a map
func attributesToMap(attrs pcommon.Map) map[string]string {
	result := make(map[string]string)
	attrs.Range(func(k string, v pcommon.Value) bool {
		result[k] = v.AsString()
		return true
	})
	return result
}

// shutdown closes the database connection
func (e *clickhouseExporter) shutdown(ctx context.Context) error {
	return e.db.Close()
}
