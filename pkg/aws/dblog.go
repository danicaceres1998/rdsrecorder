package aws

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"regexp"
	"sync"
	"time"

	"rdsrecorder/pkg/logger"
	"rdsrecorder/pkg/metrics"
	pHelper "rdsrecorder/pkg/processhelper"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/stephenafamo/kronika"
)

var (
	intervalLogSync = 1 * time.Hour
)

const (
	maxAmountLogFiles = 170 // 24(hours) * 7(days) = 168 Max Amount of log files
	startToken        = "0"
)

func StreamLogFiles(rdsClient RDSClient, s3Client S3BucketClient, dbIdentifier string, startAt, endAt time.Time) {
	// Config timing
	startAt, endAt = startAt.Add(1*time.Second), endAt.Add(2*time.Second)

	ctx, cancel := context.WithCancel(rdsClient.GetContext())
	go func() {
		// Emergency exit
		timer := time.NewTimer(time.Until(endAt))
		defer timer.Stop()

		<-timer.C // Waiting to the timer
		cancel()
	}()
	rdsClient.SetContext(ctx)
	s3Client.SetContext(ctx)

	var wg sync.WaitGroup
	for t := range kronika.Every(ctx, startAt, intervalLogSync) {
		wg.Add(1)
		go func(t time.Time) {
			defer wg.Done()

			logger.Log(logger.Debug, "new sync process started", "time", t.String())
			DownloadLogsInterval(rdsClient, s3Client, dbIdentifier, true, t.Add((-1 * intervalLogSync)), t)

			if t.After(endAt) {
				cancel()
				return
			}
			logger.Log(logger.Info, "waiting to the next sync", "time", t.Add(intervalLogSync).String())
		}(t)
	}

	wg.Wait()
}

func DownloadLogsInterval(rdsClient RDSClient, s3Client S3BucketClient, dbIdentifier string, strictInterval bool, start, finish time.Time) error {
	logFiles, err := describeLogFiles(rdsClient, dbIdentifier)
	if err != nil {
		return err
	}

	// Round the startAt datetime
	start = func(t time.Time) time.Time {
		if strictInterval {
			return t
		}
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	}(start)

	filteredLogs := make([]string, 0, len(logFiles))
	for _, file := range logFiles {
		dateFile, err := pHelper.FindDateTimeFromLogFile(file)
		if err != nil {
			logger.Log(logger.Error, err.Error())
			continue
		}

		if pHelper.TimeBetween(dateFile, start, finish) {
			filteredLogs = append(filteredLogs, file)
		}
	}

	if size := len(filteredLogs); size == 0 {
		logger.Log(
			logger.Info,
			"no files found for the provided interval",
			"startAt", start.String(),
			"endAt", finish.String(),
		)
		return nil
	}
	logger.Log(logger.Debug, "downloading logs by an interval", "start", start, "end", finish)

	// Downloading all files
	var wg sync.WaitGroup
	parallel, total := 5, len(filteredLogs)
	maxParallel := make(chan struct{}, parallel)
	for i := 0; i < parallel; i++ {
		maxParallel <- struct{}{}
	}

	for i, fileName := range filteredLogs {
		<-maxParallel
		wg.Add(1)
		go func(idx int, name string) {
			defer func() {
				wg.Done()
				maxParallel <- struct{}{}
			}()

			startSyncLogProcess(rdsClient, s3Client, dbIdentifier, name)
			logger.Log(logger.Info, "file sync completed", "file_number", fmt.Sprintf("%d/%d", idx, total))
		}(i+1, fileName)
	}

	// Waiting to all process to finish
	wg.Wait()
	return nil
}

func GetIntervalSync() time.Duration {
	return intervalLogSync
}

// Private Functions //

func describeLogFiles(client RDSClient, dbIdentifier string) ([]string, error) {
	currentToken, files := startToken, make([]string, 0, maxAmountLogFiles)
	inputParams := rds.DescribeDBLogFilesInput{
		DBInstanceIdentifier: &dbIdentifier,
		Marker:               &currentToken,
	}
	regx := regexp.MustCompile(`^.+\.csv`)

	for {
		logFiles, err := client.DescribeDBLogFiles(&inputParams)
		if err != nil {
			return nil, err
		}

		for _, file := range logFiles.DescribeDBLogFiles {
			if match := regx.MatchString(*file.LogFileName); match {
				files = append(files, *file.LogFileName)
			}
		}

		if logFiles.Marker == nil || *logFiles.Marker == currentToken {
			break
		}
		currentToken = *logFiles.Marker
	}

	return files, nil
}

func downloadLogFile(client RDSClient, dbIdentifier, logFileName string) (*os.File, error) {
	currentToken := startToken
	inputParams := rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: &dbIdentifier,
		LogFileName:          &logFileName,
		Marker:               &currentToken,
		NumberOfLines:        awsSDK.Int32(1450), // Number of lines for data without truncation
	}

	tmpFile, err := os.CreateTemp("/var/tmp", fmt.Sprintf("rds-log-%s-", pHelper.GetProcessID(client.GetContext())))
	if err != nil {
		return nil, err
	}
	writer := bufio.NewWriter(tmpFile)
	defer writer.Flush()

	for {
		downloadedFile, err := client.DownloadDBLogFilePortion(&inputParams)
		if err != nil {
			return tmpFile, err
		}

		if _, err = writer.WriteString(*downloadedFile.LogFileData); err != nil {
			return tmpFile, err
		}

		if downloadedFile.Marker == nil || *downloadedFile.Marker == currentToken {
			break
		}
		currentToken = *downloadedFile.Marker
	}

	return tmpFile, nil
}

func startSyncLogProcess(rdsClient RDSClient, s3Client S3BucketClient, dbIdentifier, targetFile string) {
	s3FileName, err := pHelper.FormatFileNameForS3(rdsClient.GetContext(), targetFile)
	if err != nil {
		logger.Log(logger.Error, fmt.Sprintf("unable to format file name, file: %s", targetFile), "error", err.Error())
		return
	}

	logger.Log(logger.Debug, "downloading a RDS log file", "file", targetFile)
	file, err := downloadLogFile(rdsClient, dbIdentifier, targetFile)
	defer pHelper.CleanTmpFile(file)
	if err != nil {
		logger.Log(logger.Error, fmt.Sprintf("unable to download log file: %s", targetFile), "error", err.Error())
		return
	}
	if stats, err := file.Stat(); err == nil {
		metrics.IncrementDownloadedLogs()
		metrics.IncrementSizeUploadedLogs(float64(stats.Size()))
	}
	logger.Log(logger.Debug, "file downloaded", "file", targetFile, "tmp", file.Name())

	logger.Log(logger.Debug, "uploading a file to S3", "file", targetFile, "s3name", s3FileName)
	if err := PushLogToBucket(s3Client, file, s3FileName, dbIdentifier); err != nil {
		logger.Log(logger.Error, fmt.Sprintf("unable to push to the bucket, file: %s", targetFile), "error", err.Error())
		return
	}
	metrics.IncrementUploadedLogs()
	logger.Log(logger.Debug, "upload to S3 done", "file", targetFile, "s3name", s3FileName)
}
