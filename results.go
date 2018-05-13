package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-playground/log"
	"github.com/pkg/errors"
)

func (r *Result) SetResult(row string) {
	// Get row value
	strValue := strings.Trim(strings.Split(row, ", ")[2], " ")
	value, err := strconv.ParseFloat(strValue, 64)
	if err != nil {
		err = errors.Wrapf(err, "Can not convert to float, setting to 0.0 (row: '%s', value: '%s').", row, strValue)
		log.Alertf("%+v", err)
		value = 0
	}

	// Save value to result
	switch {
	case strings.HasPrefix(row, "[OVERALL], Throughput(ops/sec)"):
		r.Throughput = value
	case strings.HasPrefix(row, "[READ]"):
		r.ReadResult.setOperationResult(row, value)
	case strings.HasPrefix(row, "[UPDATE]"):
		r.UpdateResult.setOperationResult(row, value)
	case strings.HasPrefix(row, "[INSERT]"):
		r.InsertResult.setOperationResult(row, value)
	case strings.HasPrefix(row, "[SCAN]"):
		r.ScanResult.setOperationResult(row, value)
	case strings.HasPrefix(row, "[READ-MODIFY-WRITE]"):
		r.RmwResult.setOperationResult(row, value)
	}

}

func (or *OperationResult) setOperationResult(row string, value float64) {
	switch {
	case strings.Contains(row, "Operations"):
		or.OperationCount = value
	case strings.Contains(row, "AverageLatency(us)"):
		or.AvgLatency = value
	case strings.Contains(row, "MinLatency(us)"):
		or.MinLatency = value
	case strings.Contains(row, "MaxLatency(us)"):
		or.MaxLatency = value
	case strings.Contains(row, "95thPercentileLatency(us)"):
		or.Per95Latency = value
	case strings.Contains(row, "99thPercentileLatency(us)"):
		or.Per99Latency = value
	}
}

type OperationResult struct {
	OperationCount float64
	AvgLatency     float64
	MinLatency     float64
	MaxLatency     float64
	Per95Latency   float64
	Per99Latency   float64
	// TODO return OK|Error
}

func (or *OperationResult) ToCsvRow() string {
	return fmt.Sprintf(
		"%f,%f,%f,%f,%f,%f",
		or.OperationCount,
		or.AvgLatency,
		or.MinLatency,
		or.MaxLatency,
		or.Per95Latency,
		or.Per99Latency,
	)
}

func buildCsvOperationHeader(prefix string) string {
	return fmt.Sprintf(
		"%s%s,%s%s,%s%s,%s%s,%s%s,%s%s",
		prefix,
		"OperationCount",
		prefix,
		"AvgLatency",
		prefix,
		"MinLatency",
		prefix,
		"MaxLatency",
		prefix,
		"Per95Latency",
		prefix,
		"Per99Latency",
	)
}

type Result struct {
	Time         time.Time
	Database     string
	Workload     string
	NodesCount   int
	ThreadsCount int
	Duration     float64
	Throughput   float64
	ReadResult   *OperationResult
	UpdateResult *OperationResult
	InsertResult *OperationResult
	ScanResult   *OperationResult
	RmwResult    *OperationResult
}

func (r *Result) ToCsvRow() string {
	return fmt.Sprintf(
		"%s,%s,%s,%d,%d,%f,%f,%s,%s,%s,%s,%s",
		r.Time.Format("2006-01-02 15:04:05"),
		r.Database,
		r.Workload,
		r.NodesCount,
		r.ThreadsCount,
		r.Duration,
		r.Throughput,
		r.ReadResult.ToCsvRow(),
		r.InsertResult.ToCsvRow(),
		r.UpdateResult.ToCsvRow(),
		r.ScanResult.ToCsvRow(),
		r.RmwResult.ToCsvRow(),
	)
}

func BuildCsvHeader() string {
	return fmt.Sprintf(
		"%s,%s,%s,%s,%s,%s",
		"timestamp,database,workload,nodes,threads,duration,throughput",
		buildCsvOperationHeader("read"),
		buildCsvOperationHeader("insert"),
		buildCsvOperationHeader("update"),
		buildCsvOperationHeader("scan"),
		buildCsvOperationHeader("rmw"),
	)
}
