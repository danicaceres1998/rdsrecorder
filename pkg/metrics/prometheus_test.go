package metrics

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestPrometheusServer(t *testing.T) {
	addr, port := "0.0.0.0", uint16(9445)
	server := StartPrometheusServer(addr, port)
	assert.Equal(t, fmt.Sprintf("%s:%d", addr, port), server.Addr)
	time.Sleep(1 * time.Second)

	resp, err := http.Get(fmt.Sprintf("http://%s:%d/metrics", addr, port))
	assert.Nil(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	err = ShutdownServer(context.Background(), server)
	assert.Nil(t, err)
}

func TestIncrementDownloadedLogs(t *testing.T) {
	c := randRange(1, 10)
	for range c {
		IncrementDownloadedLogs()
	}
	assert.Equal(t, float64(c), testutil.ToFloat64(downloadedLogsTotal))
}

func TestIncrementUploadLogs(t *testing.T) {
	c := randRange(1, 10)
	for range c {
		IncrementUploadedLogs()
	}
	assert.Equal(t, float64(c), testutil.ToFloat64(uploadedS3LogsTotal))
}

func TestIncrementSizeUploadedLogs(t *testing.T) {
	c, size, total := randRange(10, 50), float64(randRange(100, 1000)), 0.0
	for range c {
		total += size
		IncrementSizeUploadedLogs(size)
	}

	assert.Equal(t, (total / megabyte), testutil.ToFloat64(sizeUploadedLogsTotal))
}

func TestGetCounters(t *testing.T) {
	expectedCounters := []string{
		"rdsrecorder_downloaded_logs_total",
		"rdsrecorder_uploaded_s3_logs_total",
		"rdsrecorder_uploaded_s3_size_logs_total",
	}
	for k, v := range GetCounters() {
		assert.Contains(t, expectedCounters, k)
		assert.Contains(t, v.Desc().String(), k)
	}
}

// Auxiliary Functions //

func randRange(min, max int) int {
	return rand.IntN(max-min) + min
}
