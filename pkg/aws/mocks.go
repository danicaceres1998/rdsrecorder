package aws

import (
	"context"
	"os"
	"rdsrecorder/pkg/logger"

	helper "rdsrecorder/pkg/processhelper"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/mock"
)

func createRDSClientMock() *RDSClientMock {
	return &RDSClientMock{
		baseClient: baseClient{
			context.WithValue(context.Background(), helper.ContextKeyPid, "ASDF1234"), *awsSDK.NewConfig(),
		},
	}
}

func createS3ClientMock(bucketName ...string) *S3BucketClientMock {
	return &S3BucketClientMock{
		baseClient: baseClient{
			context.WithValue(context.Background(), helper.ContextKeyPid, "ASDF1234"), *awsSDK.NewConfig(),
		},
		bucketName: func() string {
			if len(bucketName) > 0 {
				return bucketName[0]
			}
			return ""
		}(),
	}
}

type RDSClientMock struct {
	mock.Mock
	baseClient
}

func (m *RDSClientMock) DescribeDBLogFiles(params *rds.DescribeDBLogFilesInput, optFns ...func(*rds.Options)) (*rds.DescribeDBLogFilesOutput, error) {
	args := m.Called(mock.Anything)
	output, ok := args[0].(*rds.DescribeDBLogFilesOutput)
	if !ok {
		logger.Log(logger.Fatal, "unable to parse the DescribeDBLogFilesOutput value")
	}
	return output, args.Error(1)
}

func (m *RDSClientMock) DownloadDBLogFilePortion(params *rds.DownloadDBLogFilePortionInput, optFns ...func(*rds.Options)) (*rds.DownloadDBLogFilePortionOutput, error) {
	args := m.Called(mock.Anything)
	output, ok := args[0].(*rds.DownloadDBLogFilePortionOutput)
	if !ok {
		logger.Log(logger.Fatal, "unable to parse the DownloadDBLogFilePortionOutput value")
	}
	return output, args.Error(1)
}

func (m *RDSClientMock) CreateDBClusterSnapshot(params *rds.CreateDBClusterSnapshotInput, optFns ...func(*rds.Options)) (*rds.CreateDBClusterSnapshotOutput, error) {
	args := m.Called(mock.Anything)
	output, ok := args[0].(*rds.CreateDBClusterSnapshotOutput)
	if !ok {
		logger.Log(logger.Fatal, "unable to parse the CreateDBClusterSnapshotOutput value")
	}
	return output, args.Error(1)
}

func (m *RDSClientMock) CreateDBSnapshot(params *rds.CreateDBSnapshotInput, optFns ...func(*rds.Options)) (*rds.CreateDBSnapshotOutput, error) {
	args := m.Called(mock.Anything)
	output, ok := args[0].(*rds.CreateDBSnapshotOutput)
	if !ok {
		logger.Log(logger.Fatal, "unable to parse the CreateDBSnapshotOutput value")
	}
	return output, args.Error(1)
}

func (m *RDSClientMock) DescribeDBInstances(params *rds.DescribeDBInstancesInput, optFns ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error) {
	args := m.Called(mock.Anything)
	output, ok := args[0].(*rds.DescribeDBInstancesOutput)
	if !ok {
		logger.Log(logger.Fatal, "unable to parse the DescribeDBInstancesOutput value")
	}
	return output, args.Error(1)
}

type S3BucketClientMock struct {
	mock.Mock
	baseClient
	bucketName string
}

func (s3Cli *S3BucketClientMock) GetBucketName() string {
	return s3Cli.bucketName
}

func (m *S3BucketClientMock) ListObjectsV2(params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	args := m.Called(mock.Anything)
	output, ok := args[0].(*s3.ListObjectsV2Output)
	if !ok {
		logger.Log(logger.Fatal, "unable to parse the ListObjectsV2Output value")
	}
	return output, args.Error(1)
}

func (m *S3BucketClientMock) VerifyBucketFolder(folder string) bool {
	args := m.Called(mock.Anything)
	return args.Bool(0)
}

func (m *S3BucketClientMock) PutObject(params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	args := m.Called(mock.Anything)
	output, ok := args[0].(*s3.PutObjectOutput)
	if !ok {
		logger.Log(logger.Fatal, "unable to parse the PutObjectOutput value")
	}
	return output, args.Error(1)
}

func (m *S3BucketClientMock) ListBuckets(params *s3.ListBucketsInput, optFns ...func(*s3.Options)) (*s3.ListBucketsOutput, error) {
	args := m.Called(mock.Anything)
	output, ok := args[0].(*s3.ListBucketsOutput)
	if !ok {
		logger.Log(logger.Fatal, "unable to parse the ListBucketsOutput value")
	}
	return output, args.Error(1)
}

func (m *S3BucketClientMock) UploadLargeFile(params *os.File, objectKey string) error {
	args := m.Called(mock.Anything)
	return args.Error(0)
}
