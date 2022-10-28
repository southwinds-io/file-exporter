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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"encoding/binary"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"southwinds.dev/artisan/core"
	resx "southwinds.dev/os"
)

const (
	timeFormat = "2006_01_02_15_04_05_999999999"
	ext        = "inproc"
	json       = "json"
	protobuf   = "proto"
)

// Marshaller configuration used for marshaling Protobuf.
var pbTracesMarshaller = ptrace.NewProtoMarshaler()
var pbMetricsMarshaller = pmetric.NewProtoMarshaler()
var pbLogsMarshaller = plog.NewProtoMarshaler()

// Marshaller configuration used for marshaling Json.
var jsonTracesMarshaller = ptrace.NewJSONMarshaler()
var jsonMetricsMarshaller = pmetric.NewJSONMarshaler()
var jsonLogsMarshaller = plog.NewJSONMarshaler()

// fileExporter is the implementation of file exporter that writes telemetry data to a file
// in Protobuf-JSON format.
type fileExporter struct {
	path              string
	mutex             sync.Mutex
	fileSizeKb        int64
	eventsPerFile     int64
	format            string
	currentEventCount int64
}

func (e *fileExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *fileExporter) ConsumeTraces(_ context.Context, td ptrace.Traces) error {

	var err error
	var buf []byte
	if strings.EqualFold(e.format, Json) {
		buf, err = jsonTracesMarshaller.MarshalTraces(td)
	} else if strings.EqualFold(e.format, Protobuf) {
		buf, err = pbTracesMarshaller.MarshalTraces(td)
	} else {
		return errors.New("invalid format, valid format value is either json or protobuf")
	}

	if err != nil {
		return err
	}
	return e.exportAsLine(buf, "traces")
}

func (e *fileExporter) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {

	var err error
	var buf []byte
	if strings.EqualFold(e.format, Json) {
		buf, err = jsonMetricsMarshaller.MarshalMetrics(md)
	} else if strings.EqualFold(e.format, Protobuf) {
		buf, err = pbMetricsMarshaller.MarshalMetrics(md)
	} else {
		return errors.New("invalid format, valid format value is either json or protobuf")
	}

	if err != nil {
		return err
	}
	return e.exportAsLine(buf, "metrics")
}

func (e *fileExporter) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	var err error
	var buf []byte
	if strings.EqualFold(e.format, Json) {
		buf, err = jsonLogsMarshaller.MarshalLogs(ld)
	} else if strings.EqualFold(e.format, Protobuf) {
		buf, err = pbLogsMarshaller.MarshalLogs(ld)
	} else {
		return errors.New("invalid format, valid format value is either json or protobuf")
	}

	if err != nil {
		return err
	}
	return e.exportAsLine(buf, "logs")
}

func (e *fileExporter) exportAsLine(buf []byte, exporttype string) error {

	// Ensure only one write operation happens at a time.
	e.mutex.Lock()
	defer e.mutex.Unlock()
	path := core.ToAbs(e.path)
	path = filepath.Join(path, exporttype)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err = os.MkdirAll(path, 0755); err != nil {
			core.ErrorLogger.Printf("failed to create path %s, error %s \n", path, err)
		}
	}
	var err error
	if e.fileSizeKb > 0 {
		err = e.writeAsPerKb(buf, exporttype, path)
	} else if e.eventsPerFile > 0 {
		err = e.writeAsPerEventCount(buf, exporttype, path)
	} else {
		return errors.New("invalid option, neither file size nor events per file is defined")
	}

	return err
}

func writeToNewFile(path string, buf []byte) error {
	core.InfoLogger.Printf("current inprocess file not found, so creating one \n")
	err := resx.WriteFile(buf, path, "")
	if err != nil {
		core.ErrorLogger.Printf("failed to write to file, %s , error %s \n", path, err)
		return err
	}
	return err
}

func (e *fileExporter) Start(context.Context, component.Host) error {
	return nil
}

// Shutdown stops the exporter and is invoked during shutdown.
func (e *fileExporter) Shutdown(context.Context) error {
	return nil
}

func (e *fileExporter) writeAsPerKb(buf []byte, exporttype, path string) error {
	// check if there is already a file with extension .inprocess, if yes use it else create new
	files, err := filepath.Glob(filepath.Join(path, fmt.Sprintf(".%s", ext)))
	if err != nil {
		core.ErrorLogger.Printf("failed to find inprocess file at path %s, error %s \n", path, err)
		return err
	}
	msize := int64(binary.Size(buf) / 1024)
	er, bol := e.isFileSizeExceeding(files, msize)
	if er != nil {
		return er
	}
	if len(files) == 0 || bol {
		if bol {
			e.renameFile(files[0])
		}
		filename := fmt.Sprintf(".%s", ext)
		path = filepath.Join(path, filename)
		err = writeToNewFile(path, buf)
		return err
	} else {
		f := files[0]
		core.InfoLogger.Printf("current inprocess file found, %s \n", f)
		file, err := os.OpenFile(f, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			core.ErrorLogger.Printf("failed to open inprocess file, %s , error %s \n", f, err)
			return err
		}
		defer file.Close()
		core.InfoLogger.Printf("writing metricss data to exitsing inprocess file, %s \n", f)
		newline := string("\n")
		if _, err := file.WriteString(newline); err != nil {
			core.ErrorLogger.Printf("failed to append new line to inprocess file, %s, error %s \n", f, err)
			return err
		}
		if _, err := file.Write(buf); err != nil {
			core.ErrorLogger.Printf("failed to write data to inprocess file, %s, error %s \n", f, err)
			return err
		}
		core.DebugLogger.Printf("size of current inprocess file and metrics data size is less than the max file size, so writing to the same file")
		//}
	}

	return nil
}

func (e *fileExporter) writeAsPerEventCount(buf []byte, exporttype, path string) error {
	// check if there is already a file with extension .inprocess, if yes use it else create new
	core.InfoLogger.Printf("writeAsPerEventCount current event count before writing event to file [ %d ]", e.currentEventCount)
	if e.currentEventCount == 0 {
		e.currentEventCount = e.currentEventCount + 1
		filename := fmt.Sprintf(".%s", ext)
		path = filepath.Join(path, filename)
		err := writeToNewFile(path, buf)
		return err
	} else {
		core.InfoLogger.Printf("writeAsPerEventCount:- before writing event, current event count [ %d ] event count defined [ %d ]",
			e.currentEventCount, e.eventsPerFile)
		files, err := filepath.Glob(filepath.Join(path, fmt.Sprintf(".%s", ext)))
		if err != nil {
			core.ErrorLogger.Printf("failed to find inprocess file at path %s, error %s \n", path, err)
			return err
		}
		f := files[0]
		core.InfoLogger.Printf("current inprocess file found, %s \n", f)
		file, err := os.OpenFile(f, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			core.ErrorLogger.Printf("failed to open inprocess file, %s , error %s \n", f, err)
			return err
		}
		defer file.Close()
		core.InfoLogger.Printf("writeAsPerEventCount:- writing event to inprocess file")
		newline := string("\n")
		if _, err := file.WriteString(newline); err != nil {
			core.ErrorLogger.Printf("failed to append new line to inprocess file, %s, error %s \n", f, err)
			return err
		}
		if _, err := file.Write(buf); err != nil {
			core.ErrorLogger.Printf("failed to write event data to inprocess file, %s, error %s \n", f, err)
			return err
		}
		e.currentEventCount = e.currentEventCount + 1
		core.DebugLogger.Printf("*incremented current event count new current event count is [ %d ] ", e.currentEventCount)
		if e.currentEventCount == e.eventsPerFile {
			core.DebugLogger.Printf("closing the current inprocess file as events per file matches")
			err = file.Close()
			if err != nil {
				core.ErrorLogger.Printf("failed to close inprocess file, %s, error %s \n", f, err)
				return err
			}

			currentTime := time.Now().UTC()
			t := currentTime.Format(timeFormat)
			var newex string
			if strings.EqualFold(e.format, Json) {
				newex = "json"
			} else if strings.EqualFold(e.format, Protobuf) {
				newex = "proto"
			} else {
				return errors.New("invalid format, valid format value is either json or protobuf")
			}
			fnew := fmt.Sprintf("%s.%s", t, newex)
			fnew = strings.Replace(f, fmt.Sprintf(".%s", ext), fnew, 1)
			core.DebugLogger.Printf("renaming old fine name %s to new file name %s", f, fnew)
			err = os.Rename(f, fnew)
			if err != nil {
				core.ErrorLogger.Printf("failed to rename inprocess file, %s \n to new file name %s \n", f, fnew)
				core.ErrorLogger.Printf("error %s ", err)
				return err
			}
			e.currentEventCount = 0
		}
	}

	return nil
}

func (e fileExporter) isFileSizeExceeding(files []string, msize int64) (error, bool) {
	// get the size of .inprocess file
	if len(files) == 0 {
		return nil, false
	}
	f := files[0]
	core.InfoLogger.Printf("current inprocess file found, %s \n", f)
	file, err := os.OpenFile(f, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		core.ErrorLogger.Printf("failed to open inprocess file, %s , error %s \n", f, err)
		return err, false
	}
	defer file.Close()
	core.DebugLogger.Printf("finding the size of current inprocess file ")
	stat, err := file.Stat()
	if err != nil {
		core.ErrorLogger.Printf("failed to get stats for inprocess file, %s , error %s \n", f, err)
		return err, false
	}
	kb := (stat.Size() / 1024)
	core.DebugLogger.Printf("before writing to inprocess file %s, file size is [ %d ]kb and metrics data size is [ %d ]kb \n", f, kb, msize)
	total := (kb + msize)
	// after adding current data to existing inprocess file, if the size of in process file exceeds
	// the maxfilesize, then close the current inprocess file and delete the extension .inprocess
	// so it will be treated as completed and ready for upload, and the current data will be written
	// to new inprocess file
	size := e.fileSizeKb

	return nil, (total > size)
}

func (e fileExporter) renameFile(f string) error {
	core.DebugLogger.Printf("closing the current inprocess file ")
	/*		err = file.Close()
			if err != nil {
				core.ErrorLogger.Printf("failed to close inprocess file, %s, error %s \n", f, err)
				return err
			}*/

	currentTime := time.Now().UTC()
	t := currentTime.Format(timeFormat)
	var newex string
	if strings.EqualFold(e.format, Json) {
		newex = "json"
	} else if strings.EqualFold(e.format, Protobuf) {
		newex = "proto"
	} else {
		return errors.New("invalid format, valid format value is either json or protobuf")
	}

	fnew := fmt.Sprintf("%s.%s", t, newex)
	fnew = strings.Replace(f, fmt.Sprintf(".%s", ext), fnew, 1)
	core.DebugLogger.Printf("renaming inprocess file [ %s ] to new file name [ %s ]", f, fnew)
	err := os.Rename(f, fnew)
	if err != nil {
		core.ErrorLogger.Printf("failed to rename inprocess file, %s \n to new file name %s \n", f, fnew)
		core.ErrorLogger.Printf("error %s ", err)
	}
	return err
}
