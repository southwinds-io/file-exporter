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
	"log"
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
	resx "southwinds.dev/os"
)

const (
	timeFormat = "2006_01_02_15_04_05_999999999"
	ext        = "inproc"
	json       = "json"
	protobuf   = "proto"
)

// Marshaller configuration used for marshaling Protobuf.
var pbTracesMarshaller = ptrace.ProtoMarshaler{}
var pbMetricsMarshaller = pmetric.ProtoMarshaler{}
var pbLogsMarshaller = plog.ProtoMarshaler{}

// Marshaller configuration used for marshaling Json.
var jsonTracesMarshaller = ptrace.JSONMarshaler{}
var jsonMetricsMarshaller = pmetric.JSONMarshaler{}
var jsonLogsMarshaller = plog.JSONMarshaler{}

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
	return e.exportAsLine(buf)
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
	return e.exportAsLine(buf)
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
	return e.exportAsLine(buf)
}

func (e *fileExporter) exportAsLine(buf []byte) error {

	// Ensure only one write operation happens at a time.
	e.mutex.Lock()
	defer e.mutex.Unlock()
	path := e.path
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err = os.MkdirAll(path, 0755); err != nil {
			log.Printf("failed to create path %s, error %s \n", path, err)
		}
	}
	var err error
	if e.fileSizeKb > 0 {
		err = e.writeAsPerKb(buf, path)
	} else if e.eventsPerFile > 0 {
		err = e.writeAsPerEventCount(buf, path)
	} else {
		return errors.New("invalid option, neither file size nor events per file is defined")
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

func (e *fileExporter) writeAsPerKb(buf []byte, path string) error {
	// check if there is already a file with extension .inprocess, if yes use it else create new
	files, err := filepath.Glob(filepath.Join(path, fmt.Sprintf(".%s", ext)))
	if err != nil {
		log.Printf("failed to find inprocess file at path %s, error %s \n", path, err)
		return err
	}
	msize := int64(binary.Size(buf) / 1024)
	er, bol := e.isFileSizeExceeding(files, msize)
	if er != nil {
		return er
	}
	if len(files) == 0 || bol {
		if bol {
			e.renameTmpFile(files[0])
		}
		filename := fmt.Sprintf(".%s", ext)
		path = filepath.Join(path, filename)
		err = resx.AppendFileBatch(buf, path, 0755)
		return err
	} else {
		f := files[0]
		if len(os.Getenv("TELE_DEBUG")) > 0 {
			log.Printf("writeAsPerKb current inprocess file found, %s \n", f)
		}
		err = resx.AppendFileBatch(buf, f, 0755)
		if err != nil {
			log.Printf("failed to write data to inprocess file, %s, error %s \n", f, err)
			return err
		}
		if len(os.Getenv("TELE_DEBUG")) > 0 {
			log.Printf("size of current inprocess file and input data size is less than the max file size, so writing to the same file")
		}
	}
	return nil
}

func (e *fileExporter) writeAsPerEventCount(buf []byte, path string) error {
	// check if there is already a file with extension .inprocess, if yes use it else create new
	if len(os.Getenv("TELE_DEBUG")) > 0 {
		log.Printf("writeAsPerEventCount current event count before writing event to file [ %d ]", e.currentEventCount)
	}
	if e.currentEventCount == 0 {
		e.currentEventCount = e.currentEventCount + 1
		filename := fmt.Sprintf(".%s", ext)
		path = filepath.Join(path, filename)
		err := resx.AppendFileBatch(buf, path, 0644)
		if err != nil {
			log.Printf("failed to append data to inprocess file at path %s, error %s \n", path, err)
			return err
		}
		if e.currentEventCount == e.eventsPerFile {
			err = e.renameTmpFile(path)
			if err != nil {
				log.Printf("failed to rename inprocess file at path %s, error %s \n", path, err)
				return err
			}
		}
		return nil
	} else {
		files, err := filepath.Glob(filepath.Join(path, fmt.Sprintf(".%s", ext)))
		if err != nil {
			log.Printf("failed to find inprocess file at path %s, error %s \n", path, err)
			return err
		}
		f := files[0]
		if len(os.Getenv("TELE_DEBUG")) > 0 {
			log.Printf("writeAsPerEventCount appending file batch to => [ %s ] \n", f)
		}
		err = resx.AppendFileBatch(buf, f, 0644)
		if err != nil {
			log.Printf("failed to append data to inprocess file, %s, error %s \n", f, err)
			return err
		}
		e.currentEventCount = e.currentEventCount + 1
		if len(os.Getenv("TELE_DEBUG")) > 0 {
			log.Printf("incremented current event count, current [ %d ], per file [ %d ] ", e.currentEventCount, e.eventsPerFile)
		}
		if e.currentEventCount == e.eventsPerFile {
			err = e.renameTmpFile(f)
			if err != nil {
				log.Printf("failed to rename inprocess file at path %s, error %s \n", path, err)
				return err
			}
		}
	}

	return nil
}

func (e *fileExporter) renameTmpFile(f string) error {
	if e.currentEventCount == e.eventsPerFile {
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
		if len(os.Getenv("TELE_DEBUG")) > 0 {
			log.Printf("renaming old fine name %s to new file name %s", f, fnew)
		}
		err := os.Rename(f, fnew)
		if err != nil {
			log.Printf("failed to rename inprocess file, %s \n to new file name %s \n", f, fnew)
			log.Printf("error %s ", err)
			return err
		}
		e.currentEventCount = 0
	}
	return nil
}

func (e *fileExporter) isFileSizeExceeding(files []string, msize int64) (error, bool) {
	// get the size of .inprocess file
	if len(files) == 0 {
		return nil, false
	}
	f := files[0]
	file, err := os.OpenFile(f, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("failed to open inprocess file, %s , error %s \n", f, err)
		return err, false
	}
	defer file.Close()
	if len(os.Getenv("TELE_DEBUG")) > 0 {
		log.Printf("finding the size of current inprocess file ")
	}
	stat, err := file.Stat()
	if err != nil {
		log.Printf("failed to get stats for inprocess file, %s , error %s \n", f, err)
		return err, false
	}
	kb := (stat.Size() / 1024)
	if len(os.Getenv("TELE_DEBUG")) > 0 {
		log.Printf("before writing to inprocess file %s, file size is [ %d ]kb and input data size is [ %d ]kb \n", f, kb, msize)
	}
	total := (kb + msize)
	// after adding current data to existing inprocess file, if the size of in process file exceeds
	// the maxfilesize, then close the current inprocess file and delete the extension .inprocess
	// so it will be treated as completed and ready for upload, and the current data will be written
	// to new inprocess file
	size := e.fileSizeKb

	return nil, (total > size)
}
