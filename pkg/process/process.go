package process

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"rdsrecorder/pkg/aws"
	"rdsrecorder/pkg/logger"
	helper "rdsrecorder/pkg/processhelper"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
)

func StartSyncProcess(ctx context.Context, cfg awsSDK.Config, dbIdentifier, startAt, endAt, bucketName string) error {
	if _, ok := os.LookupEnv(aws.BucketEnvVar); !ok && bucketName == "" {
		return errors.New("you must provide the bucket identifier")
	}

	var (
		wg  sync.WaitGroup
		err error
	)
	// Sarting the DB Snapshot
	wg.Add(1)
	go func() {
		defer wg.Done()

		if helper.IsRecovery(ctx) {
			return
		}
		_ = createSnapshot(ctx, cfg, dbIdentifier, startAt)
		logger.Log(logger.Info, "snapshot process is finished")
	}()

	// Starting the Log sync
	wg.Add(1)
	go func() {
		defer wg.Done()

		if helper.IsRecovery(ctx) {
			// reSyncLogs(ctx, cfg, dbIdentifier, startAt, endAt, bucketName)
		} else {
			err = syncLogs(ctx, cfg, dbIdentifier, startAt, endAt, bucketName)
		}
		logger.Log(logger.Info, "log sync process is finished")
	}()

	wg.Wait()
	logger.Log(logger.Info, "all processes were finished")
	if err != nil {
		return fmt.Errorf(
			"the process finished with an error, message_error: '%s'",
			err.Error(),
		)
	}
	return nil
}

func StartSnapshotProcess(ctx context.Context, cfg awsSDK.Config, dbIdentifier, startAt string) error {
	var (
		start time.Time
		err   error
	)

	// Date Parsing
	if start, err = parseTimestamp(startAt); err != nil {
		logger.Log(
			logger.Fatal, "invalid input for --start flag",
			"error", err, "input", start,
		)
		return err
	}

	return createSnapshot(
		ctx, cfg, dbIdentifier,
		func() string {
			if start.IsZero() {
				return helper.CurrentTime().Add(3 * time.Second).Format(helper.TimeStampFormat)
			}
			return startAt
		}(),
	)
}

func CreateContextWithPid(ctx context.Context) (context.Context, error) {
	if pid, ok := os.LookupEnv("rdsrecorder_PROCESS_ID"); ok {
		ctx = context.WithValue(ctx, helper.ContextKeyPid, pid)
		ctx = context.WithValue(ctx, helper.ContextKeyPidExternal, true)
		return ctx, nil
	}

	processId, err := func(n int) (string, error) {
		bytes := make([]byte, n)
		if _, err := rand.Read(bytes); err != nil {
			return "", err
		}
		return hex.EncodeToString(bytes), nil
	}(12)

	if err != nil {
		return nil, err
	}

	return context.WithValue(ctx, helper.ContextKeyPid, processId), nil
}

// Private Functions //

func syncLogs(ctx context.Context, cfg awsSDK.Config, dbIdentifier, startAt, endAt, bucketName string) error {
	var (
		err           error
		start, finish time.Time
		currentT      time.Time = helper.CurrentTime()
	)

	// Date Parsing
	if start, err = parseTimestamp(startAt); err != nil || start.IsZero() {
		logger.Log(logger.Error, "invalid input for --start flag", "error", err, "input", start)
		return err
	} else if finish, err = parseTimestamp(endAt); err != nil || finish.IsZero() {
		logger.Log(logger.Error, "invalid input for --finish flag", "error", err, "input", finish)
		return err
	}

	// Dates Validation
	if err := helper.ValidateStartFinishInterval(start, finish); err != nil {
		logger.Log(logger.Error, "the start at & end at interval are not valid", "error", err.Error())
		return err
	} else if err := helper.Validate7DaysInterval(start); err != nil {
		logger.Log(logger.Error, "the start at date is before the 7 days DB log retention", "error", err.Error())
		return err
	} else if err := helper.ValidateTimeZone(start, finish); err != nil {
		logger.Log(logger.Error, "the start or finish datetime is not on UTC timezone", "error", err.Error())
		return err
	}

	// Business Logic //
	rdsClient := aws.CreateRDSClient(ctx, cfg)
	s3Client := aws.CreateS3Client(ctx, cfg, bucketName)
	subStart, subFinish := start.Sub(currentT), finish.Sub(currentT)

	// Verify Bucket //
	if ok := aws.VerifyBucket(s3Client); !ok {
		return fmt.Errorf("no bucket found with name: %s", bucketName)
	}

	// Wait & Sync //
	// Start Date is in the future/current time
	if subStart >= 0 {
		logger.Log(logger.Debug, "starting process: Wait & Sync")
		aws.StreamLogFiles(rdsClient, s3Client, dbIdentifier, start, finish)
		return nil
	}

	// Download the interval & finish //
	// Start Date is on the past and the End Date is on the past/current time
	if subFinish <= 0 {
		logger.Log(logger.Debug, "starting process: Download Interval")
		if err := aws.DownloadLogsInterval(rdsClient, s3Client, dbIdentifier, true, start, finish); err != nil {
			logger.Log(logger.Error, "the download log interval function finished with an error", "error", err.Error())
			return err
		}
		return nil
	}

	// Download the interval & Sync //
	// Start Date is on the past and the end date is on the future
	logger.Log(logger.Debug, "starting process: Download Interval & Sync")
	doneDownload, startTime := make(chan struct{}), helper.CurrentTime()
	go func() {
		// Download until the third to last log
		err = aws.DownloadLogsInterval(rdsClient, s3Client, dbIdentifier, false, start.Add(-1*aws.GetIntervalSync()), startTime.Add(-1*aws.GetIntervalSync()))
		if err != nil {
			logger.Log(logger.Error, "the download log interval function finished with an error", "error", err.Error())
		} else {
			logger.Log(logger.Info, "the download log interval function is finished")
		}

		doneDownload <- struct{}{}
		close(doneDownload)
	}()

	aws.StreamLogFiles(rdsClient, s3Client, dbIdentifier, startTime, finish)
	<-doneDownload // Waiting to the download interval
	return err
}

func createSnapshot(ctx context.Context, cfg awsSDK.Config, dbIdentifier, startAt string) error {
	var (
		err   error
		start time.Time
	)

	// Date Parsing
	if start, err = parseTimestamp(startAt); err != nil {
		logger.Log(logger.Error, "invalid input for --start flag", "error", err, "input", start)
		return err
	}
	if start.IsZero() {
		start = helper.CurrentTime()
	}

	// Date Validation
	if time.Until(start) < 0 {
		msg := "unable to create a snapshot from past data"
		logger.Log(logger.Error, "the start at time is on the past", "error", msg)
		return errors.New(msg)
	} else if err := helper.ValidateTimeZone(start); err != nil {
		logger.Log(logger.Error, "invalid timezone", "error", err.Error())
		return err
	}

	// Business Logic //
	client := aws.CreateRDSClient(ctx, cfg)
	if err := aws.CreateDBSnapshot(client, dbIdentifier, start); err != nil {
		logger.Log(logger.Error, "unable to create the snapshot", "error", err.Error())
		return err
	}
	return nil
}

func parseTimestamp(in string) (time.Time, error) {
	if in == "" {
		return time.Time{}, nil
	}

	t, err := time.Parse(helper.TimeStampFormat, in)
	if err != nil {
		return time.Time{},
			fmt.Errorf("must be a valid timestamp (%s), error: %s", helper.TimeStampFormat, err.Error())
	}

	return t, nil
}
