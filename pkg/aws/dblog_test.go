package aws

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"rdsrecorder/pkg/metrics"
	helper "rdsrecorder/pkg/processhelper"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDescribeLogFiles(t *testing.T) {
	dbIdentifier, data := "test-db", []struct {
		name     string
		files    []string
		expected []string
		err      error
		marker   *string
	}{
		{"sucess-listing", []string{"test_file_1.csv"}, []string{"test_file_1.csv"}, nil, nil},
		{"sucess-listing-marker", []string{"test_file_1.csv"}, []string{"test_file_1.csv", "test_file_1.csv"}, nil, awsSDK.String("next-marker")},
		{"error-describe", nil, nil, errors.New("unable to fetch files"), nil},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			clientMock := createRDSClientMock()
			clientMock.On("DescribeDBLogFiles", mock.Anything).Return(
				&rds.DescribeDBLogFilesOutput{
					DescribeDBLogFiles: func() []types.DescribeDBLogFilesDetails {
						if d.err != nil {
							return nil
						}
						return createListFiles(d.files)
					}(),
					Marker: d.marker,
				},
				d.err,
			)

			result, err := describeLogFiles(clientMock, dbIdentifier)
			if d.err != nil {
				assert.Error(t, err)
				assert.Nil(t, result)
				return
			}
			assert.Nil(t, err)
			assert.ElementsMatch(t, d.expected, result)
		})
	}
}

func TestDownloadLogFile(t *testing.T) {
	dbIdentifier, data := "test-db", []struct {
		name        string
		fileContent string
		expected    string
		err         error
		marker      *string
	}{
		{"success-download", "Hello World!", "Hello World!", nil, nil},
		{"error-download", "", "", errors.New("unable to download"), nil},
		{"pending-data-to-download", "Hello World!", "Hello World!Hello World!", nil, awsSDK.String("next-token")},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			clientMock := createRDSClientMock()
			clientMock.On("DownloadDBLogFilePortion", mock.Anything).Return(
				&rds.DownloadDBLogFilePortionOutput{
					LogFileData: &d.fileContent, Marker: d.marker,
				},
				d.err,
			)
			file, err := downloadLogFile(clientMock, dbIdentifier, "test-file")
			if d.err != nil {
				assert.Error(t, err)
				assert.True(t, file != nil)
				helper.CleanTmpFile(file)
				return
			}
			content, _ := os.ReadFile(file.Name())
			assert.Equal(t, d.expected, string(content))
			assert.Contains(t, file.Name(), fmt.Sprintf("rds-log-%s-", helper.GetProcessID(clientMock.GetContext())))
			// Clean tmp file
			helper.CleanTmpFile(file)
		})
	}
}

func TestStartSyncLogProcess(t *testing.T) {
	dbIdentifier, targetFile := "db-test", "error/postgresql.log.2024-02-23-0830.csv"
	data := []struct {
		name            string
		rdsErr          error
		s3Err           error
		invalidFileName bool
	}{
		{"success-flow", nil, nil, false},
		{"unable-to-download", errors.New("unable to download"), nil, false},
		{"unable-to-upload", nil, errors.New("unable to upload"), false},
		{"invalid-log-name", nil, nil, true},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			cliRDSMock, cliBucketMock := createRDSClientMock(), createS3ClientMock()
			cliRDSMock.On("DownloadDBLogFilePortion", mock.Anything).Return(
				&rds.DownloadDBLogFilePortionOutput{
					LogFileData: awsSDK.String("Hello World!"), Marker: nil,
				},
				d.rdsErr,
			)
			cliBucketMock.On("UploadLargeFile", mock.Anything).Return(d.s3Err)
			cliBucketMock.On("ListObjectsV2", mock.Anything).Return(
				&s3.ListObjectsV2Output{
					Contents: []s3Types.Object{{}},
				},
				nil,
			)

			startSyncLogProcess(
				cliRDSMock, cliBucketMock, dbIdentifier,
				func() string {
					if d.invalidFileName {
						return "test-file"
					}
					return targetFile
				}(),
			)
			if !d.invalidFileName {
				cliRDSMock.AssertCalled(t, "DownloadDBLogFilePortion")
				if d.rdsErr == nil {
					cliBucketMock.AssertCalled(t, "UploadLargeFile")
				}
			}
		})
	}

	// Metrics validation
	counters := metrics.GetCounters()
	assert.Equal(t, float64(2), testutil.ToFloat64(counters["rdsrecorder_downloaded_logs_total"]))
	assert.Equal(t, float64(1), testutil.ToFloat64(counters["rdsrecorder_uploaded_s3_logs_total"]))
	assert.Equal(t, "0.000023", fmt.Sprintf("%2f", testutil.ToFloat64(counters["rdsrecorder_uploaded_s3_size_logs_total"])))
}

func TestDownloadLogsInterval(t *testing.T) {
	dbIdentifier := "test-db"
	data := []struct {
		name           string
		err            error
		strictInterval bool
		// DescribeDbLogFiles
		files      []struct{}
		descMarker *string
		descErr    error
		// DownlaodDbLogFile
		fileContent string
		downMarker  *string
		downErr     error
		//UploadFiles
		uploadErr error
		// ListObjectsV2
		listObjContents []s3Types.Object
		listErr         error
	}{
		{
			"success-download", nil, true,
			[]struct{}{{}}, nil, nil,
			"Hello World!", nil, nil,
			nil,
			[]s3Types.Object{{}}, nil,
		},
		{
			"unable-describe-logs", errors.New("unalbe-describe-logs"), true,
			[]struct{}{}, nil, errors.New("unalbe-describe-logs"),
			"", nil, nil,
			nil,
			[]s3Types.Object{}, nil,
		},
		{
			"non-strict-interval-empty-filtered-logs", nil, false,
			[]struct{}{}, nil, nil,
			"Hello World!", nil, errors.New("skip-download"),
			nil,
			[]s3Types.Object{}, nil,
		},
	}
	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			// Mock Config
			rdsCliMock, s3CliMock := createRDSClientMock(), createS3ClientMock()
			currTime := helper.CurrentTime()
			rdsCliMock.On("DescribeDBLogFiles", mock.Anything).Return(
				&rds.DescribeDBLogFilesOutput{
					DescribeDBLogFiles: func() []types.DescribeDBLogFilesDetails {
						files, fct := make([]string, 0, cap(d.files)), currTime.Add(1*time.Minute)
						for range d.files {
							files = append(
								files, "error/postgresql.log."+fmt.Sprintf(
									"%d-%02d-%02d-%02d%02d",
									fct.Year(), fct.Month(), fct.Day(),
									fct.Hour(), fct.Minute(),
								)+".csv",
							)
						}
						return createListFiles(files)
					}(),
					Marker: d.descMarker,
				},
				d.descErr,
			)
			rdsCliMock.On("DownloadDBLogFilePortion", mock.Anything).Return(
				&rds.DownloadDBLogFilePortionOutput{
					LogFileData: awsSDK.String("Hello World!"), Marker: d.downMarker,
				},
				d.downErr,
			)
			s3CliMock.On("UploadLargeFile", mock.Anything).Return(d.uploadErr)
			s3CliMock.On("ListObjectsV2", mock.Anything).Return(
				&s3.ListObjectsV2Output{
					Contents: d.listObjContents,
				},
				d.listErr,
			)

			// Testing //
			err := DownloadLogsInterval(rdsCliMock, s3CliMock, dbIdentifier, d.strictInterval, currTime, currTime.Add(5*time.Minute))
			if d.err != nil {
				assert.Error(t, err)
			}
			rdsCliMock.AssertCalled(t, "DescribeDBLogFiles")
			if d.descErr == nil && len(d.files) > 0 {
				rdsCliMock.AssertCalled(t, "DownloadDBLogFilePortion")
				if d.downErr == nil {
					s3CliMock.AssertCalled(t, "UploadLargeFile")
				}
			}
		})
	}
}

func TestStreamLogFiles(t *testing.T) {
	dbIdentifier := "test-db"
	data := []struct {
		name string
		err  error
		// DescribeDbLogFiles
		files      []struct{}
		descMarker *string
		descErr    error
	}{
		{
			"success-download", nil,
			[]struct{}{{}}, nil, nil,
		},
	}

	intervalLogSync = 100 * time.Millisecond
	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			rdsCliMock, s3CliMock := createRDSClientMock(), createS3ClientMock()
			currTime := helper.CurrentTime()
			rdsCliMock.On("DescribeDBLogFiles", mock.Anything).Return(
				&rds.DescribeDBLogFilesOutput{
					DescribeDBLogFiles: func() []types.DescribeDBLogFilesDetails {
						files, fct := make([]string, 0, cap(d.files)), currTime
						for range d.files {
							files = append(
								files, "error/postgresql.log."+fmt.Sprintf(
									"%d-%02d-%02d-%02d%02d",
									fct.Year(), fct.Month(), fct.Day(),
									fct.Hour(), fct.Minute(),
								)+".csv",
							)
						}
						return createListFiles(files)
					}(),
					Marker: d.descMarker,
				},
				d.descErr,
			)

			StreamLogFiles(rdsCliMock, s3CliMock, dbIdentifier, currTime, currTime)
			assert.GreaterOrEqual(
				t, func() int {
					var actualCalls int
					for _, call := range rdsCliMock.Calls {
						if call.Method == "DescribeDBLogFiles" {
							actualCalls++
						}
					}
					return actualCalls
				}(),
				10, // 1 second = 10 * miliseconds(100)
			)
		})
	}
}

// Auxiliary functions //

func createListFiles(files []string) []types.DescribeDBLogFilesDetails {
	fDetails := make([]types.DescribeDBLogFilesDetails, 0, cap(files))
	for _, f := range files {
		fDetails = append(fDetails, types.DescribeDBLogFilesDetails{
			LogFileName: awsSDK.String(f),
		})
	}
	return fDetails
}
