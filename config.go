/*
  open telemetry file exporter for pilot
  © 2018-Present - SouthWinds Tech Ltd - www.southwinds.io
  Licensed under the Apache License, Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0
  Contributors to this project, hereby assign copyright in this code to the project,
  to be licensed under the same terms as the rest of the code.
*/

package fileexporter

import (
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/config"
)

const (
	maxfilesize      = int64(100) // 100kb
	maxEventsPerFile = 1
	fileSize         = "fileSizeKb"
	eventsSize       = "EventsPerFile"
	Json             = "json"
	Protobuf         = "protobuf"
)

// Config defines configuration for file exporter.
type Config struct {
	config.ExporterSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct

	// Path of the file to write to. Path is relative to current directory.
	Path          string `mapstructure:"path"`
	FileSizeKb    int64  `mapstructure:"filesizekb"`
	EventsPerFile int64  `mapstructure:"eventsPerFile"`
	Format        string `mapstructure:"format"`
	Default       string `mapstructure:"default"`
}

var _ config.Exporter = (*Config)(nil)

// Validate checks if the exporter configuration is valid
func (cfg *Config) Validate() error {

	if len(cfg.Path) == 0 {
		return errors.New("path must be defined")
	}
	if len(cfg.Format) == 0 {
		return errors.New("format must be defined as either json or protobuf")
	}

	if !strings.EqualFold(cfg.Format, Json) && !strings.EqualFold(cfg.Format, Protobuf) {
		return fmt.Errorf("invalid format [%s] , valid format value is either [ json or protobuf]", cfg.Format)
	}

	if cfg.FileSizeKb > 0 && cfg.EventsPerFile > 0 && len(cfg.Default) > 0 {
		return fmt.Errorf("mention either fileSizeKb or eventsPerFile or default in telem.yaml file")
	} else if cfg.FileSizeKb > 0 && cfg.EventsPerFile > 0 {
		return fmt.Errorf("mention either fileSizeKb or eventsPerFile")
	} else if cfg.FileSizeKb > 0 && len(cfg.Default) > 0 {
		return fmt.Errorf("mention either fileSizeKb or default")
	} else if len(cfg.Default) > 0 && cfg.EventsPerFile > 0 {
		return fmt.Errorf("mention either default or eventsPerFile")
	}

	if cfg.FileSizeKb == 0 && cfg.EventsPerFile == 0 && len(cfg.Default) == 0 {
		return fmt.Errorf("fileSizeKb or eventsPerFile or default value must be defined in telem.yaml file")
	}
	if cfg.FileSizeKb == 0 && cfg.EventsPerFile == 0 {
		if strings.EqualFold(cfg.Default, fileSize) {
			cfg.FileSizeKb = maxfilesize
		} else if strings.EqualFold(cfg.Default, eventsSize) {
			cfg.EventsPerFile = maxEventsPerFile
		} else {
			return fmt.Errorf("invalid default value defined in telem.yaml file, valid values are [ %s or %s ]", fileSize, eventsSize)
		}
	}

	return nil
}
