package metrics

import (
	"context"
	"fmt"
	"net/http"
	"rdsrecorder/pkg/logger"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const megabyte = 1024 * 1024

var (
	downloadedLogsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rdsrecorder_downloaded_logs_total",
		Help: "Total amount of log files downloaded from RDS",
	})

	uploadedS3LogsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rdsrecorder_uploaded_s3_logs_total",
		Help: "Total amount of log files uploaded to S3 Bucket",
	})

	sizeUploadedLogsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rdsrecorder_uploaded_s3_size_logs_total",
		Help: "Total amount of MB uploaded to the S3 Bucket",
	})
)

func StartPrometheusServer(address string, port uint16) *http.Server {
	// Server Configuration
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%v", address, port),
		Handler: mux,
	}

	// Starting the servier
	go func() {
		logger.Log(logger.Info, "metrics.listen", "address", address, "port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Log(logger.Error, "server.not-started", "message", err.Error())
			return
		}
	}()

	return server
}

func ShutdownServer(ctx context.Context, server *http.Server) error {
	// Waiting for Prometheus to get all the data left
	time.Sleep(1 * time.Second)

	// Shutdown the server gracefully
	if err := server.Shutdown(ctx); err != nil {
		return err
	}

	return nil
}

func IncrementDownloadedLogs() {
	downloadedLogsTotal.Inc()
}

func IncrementUploadedLogs() {
	uploadedS3LogsTotal.Inc()
}

func IncrementSizeUploadedLogs(sizeBytes float64) {
	sizeUploadedLogsTotal.Add(sizeBytes / megabyte)
}

func GetCounters() map[string]prometheus.Counter {
	return map[string]prometheus.Counter{
		"rdsrecorder_downloaded_logs_total":       downloadedLogsTotal,
		"rdsrecorder_uploaded_s3_logs_total":      uploadedS3LogsTotal,
		"rdsrecorder_uploaded_s3_size_logs_total": sizeUploadedLogsTotal,
	}
}
