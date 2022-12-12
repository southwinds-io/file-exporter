/*
  open telemetry file exporter for pilot
  © 2018-Present - SouthWinds Tech Ltd - www.southwinds.io
  Licensed under the Apache License, Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0
  Contributors to this project, hereby assign copyright in this code to the project,
  to be licensed under the same terms as the rest of the code.
*/

package fileexporter

import (
	"context"

	"go.opentelemetry.io/collector/config"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	// The value of "type" key in configuration.
	typeStr = "file"
	// The stability level of the exporter.
	stability = component.StabilityLevelAlpha
)

// NewFactory creates a factory for OTLP exporter.
func NewFactory() component.ExporterFactory {
	return component.NewExporterFactory(
		typeStr,
		createDefaultConfig,
		component.WithTracesExporter(createTracesExporter, stability),
		component.WithMetricsExporter(createMetricsExporter, stability),
		component.WithLogsExporter(createLogsExporter, stability))
}

func createDefaultConfig() component.ExporterConfig {

	return &Config{
		ExporterSettings: config.NewExporterSettings(component.NewID(typeStr)),
	}
}

func createTracesExporter(
	ctx context.Context,
	set component.ExporterCreateSettings,
	cfg component.ExporterConfig,
) (component.TracesExporter, error) {
	fe := exporters.GetOrAdd(cfg, func() component.Component {
		return &fileExporter{path: cfg.(*Config).Path, fileSizeKb: cfg.(*Config).FileSizeKb,
			eventsPerFile: cfg.(*Config).EventsPerFile, format: cfg.(*Config).Format}
	})
	return exporterhelper.NewTracesExporter(
		ctx,
		set,
		cfg,
		fe.Unwrap().(*fileExporter).ConsumeTraces,
		exporterhelper.WithStart(fe.Start),
		exporterhelper.WithShutdown(fe.Shutdown),
	)
}

func createMetricsExporter(
	ctx context.Context,
	set component.ExporterCreateSettings,
	cfg component.ExporterConfig,
) (component.MetricsExporter, error) {
	fe := exporters.GetOrAdd(cfg, func() component.Component {
		return &fileExporter{path: cfg.(*Config).Path, fileSizeKb: cfg.(*Config).FileSizeKb,
			eventsPerFile: cfg.(*Config).EventsPerFile, format: cfg.(*Config).Format}
	})
	return exporterhelper.NewMetricsExporter(
		ctx,
		set,
		cfg,
		fe.Unwrap().(*fileExporter).ConsumeMetrics,
		exporterhelper.WithStart(fe.Start),
		exporterhelper.WithShutdown(fe.Shutdown),
	)
}

func createLogsExporter(
	ctx context.Context,
	set component.ExporterCreateSettings,
	cfg component.ExporterConfig,
) (component.LogsExporter, error) {
	fe := exporters.GetOrAdd(cfg, func() component.Component {
		return &fileExporter{path: cfg.(*Config).Path, fileSizeKb: cfg.(*Config).FileSizeKb,
			eventsPerFile: cfg.(*Config).EventsPerFile, format: cfg.(*Config).Format}
	})
	return exporterhelper.NewLogsExporter(
		ctx,
		set,
		cfg,
		fe.Unwrap().(*fileExporter).ConsumeLogs,
		exporterhelper.WithStart(fe.Start),
		exporterhelper.WithShutdown(fe.Shutdown),
	)
}

// This is the map of already created File exporters for particular configurations.
// We maintain this map because the Factory is asked trace and metric receivers separately
// when it gets CreateTracesReceiver() and CreateMetricsReceiver() but they must not
// create separate objects, they must use one Receiver object per configuration.
var exporters = NewSharedComponents()
