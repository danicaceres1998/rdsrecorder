package aws

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"rdsrecorder/pkg/logger"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Interfaces //

type clientBase interface {
	GetConfig() awsSDK.Config
	GetContext() context.Context
	SetContext(context.Context)
}

type RDSClient interface {
	clientBase
	DescribeDBLogFiles(*rds.DescribeDBLogFilesInput, ...func(*rds.Options)) (*rds.DescribeDBLogFilesOutput, error)
	DownloadDBLogFilePortion(*rds.DownloadDBLogFilePortionInput, ...func(*rds.Options)) (*rds.DownloadDBLogFilePortionOutput, error)
	CreateDBClusterSnapshot(*rds.CreateDBClusterSnapshotInput, ...func(*rds.Options)) (*rds.CreateDBClusterSnapshotOutput, error)
	CreateDBSnapshot(*rds.CreateDBSnapshotInput, ...func(*rds.Options)) (*rds.CreateDBSnapshotOutput, error)
	DescribeDBInstances(*rds.DescribeDBInstancesInput, ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error)
}

type S3BucketClient interface {
	clientBase
	GetBucketName() string
	ListObjectsV2(*s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	PutObject(*s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	UploadLargeFile(*os.File, string) error
	ListBuckets(*s3.ListBucketsInput, ...func(*s3.Options)) (*s3.ListBucketsOutput, error)
}

// Structures //

type baseClient struct {
	ctx context.Context
	cfg awsSDK.Config
}

func (bc baseClient) GetConfig() awsSDK.Config {
	return bc.cfg
}

func (bc baseClient) GetContext() context.Context {
	return bc.ctx
}

func (bc *baseClient) SetContext(ctx context.Context) {
	bc.ctx = ctx
}

type rdsClient struct {
	baseClient
}

func (rdsCli rdsClient) CreateDBClusterSnapshot(params *rds.CreateDBClusterSnapshotInput, optFns ...func(*rds.Options)) (*rds.CreateDBClusterSnapshotOutput, error) {
	client := rds.NewFromConfig(rdsCli.cfg)
	return client.CreateDBClusterSnapshot(rdsCli.ctx, params, optFns...)
}

func (rdsCli rdsClient) CreateDBSnapshot(params *rds.CreateDBSnapshotInput, optFns ...func(*rds.Options)) (*rds.CreateDBSnapshotOutput, error) {
	client := rds.NewFromConfig(rdsCli.cfg)
	return client.CreateDBSnapshot(rdsCli.ctx, params, optFns...)
}

func (rdsCli rdsClient) DescribeDBInstances(params *rds.DescribeDBInstancesInput, optFns ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error) {
	client := rds.NewFromConfig(rdsCli.cfg)
	return client.DescribeDBInstances(rdsCli.ctx, params, optFns...)
}

func (logCli rdsClient) DescribeDBLogFiles(params *rds.DescribeDBLogFilesInput, optFns ...func(*rds.Options)) (*rds.DescribeDBLogFilesOutput, error) {
	client := rds.NewFromConfig(logCli.cfg)
	return client.DescribeDBLogFiles(logCli.ctx, params, optFns...)
}

func (logCli rdsClient) DownloadDBLogFilePortion(params *rds.DownloadDBLogFilePortionInput, optFns ...func(*rds.Options)) (*rds.DownloadDBLogFilePortionOutput, error) {
	client := rds.NewFromConfig(logCli.cfg)
	return client.DownloadDBLogFilePortion(logCli.ctx, params, optFns...)
}

type s3BucketClient struct {
	baseClient
	bucketName string
}

func (s3Cli s3BucketClient) GetBucketName() string {
	return s3Cli.bucketName
}

func (s3Cli s3BucketClient) ListObjectsV2(params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	client := s3.NewFromConfig(s3Cli.cfg)
	return client.ListObjectsV2(s3Cli.ctx, params, optFns...)
}

func (s3Cli s3BucketClient) PutObject(params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	client := s3.NewFromConfig(s3Cli.cfg)
	return client.PutObject(s3Cli.ctx, params, optFns...)
}

func (s3Cli s3BucketClient) ListBuckets(params *s3.ListBucketsInput, optFns ...func(*s3.Options)) (*s3.ListBucketsOutput, error) {
	client := s3.NewFromConfig(s3Cli.cfg)
	return client.ListBuckets(s3Cli.ctx, params, optFns...)
}

func (s3Cli s3BucketClient) UploadLargeFile(file *os.File, objectKey string) error {
	client := s3.NewFromConfig(s3Cli.cfg)
	fileContent, err := os.ReadFile(file.Name())
	if err != nil {
		return err
	}
	largeBuffer := bytes.NewReader(fileContent)

	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = 10 * 1024 * 1024 // 10 MBs
	})
	_, err = uploader.Upload(s3Cli.GetContext(), &s3.PutObjectInput{
		Bucket: &s3Cli.bucketName,
		Key:    awsSDK.String(objectKey),
		Body:   largeBuffer,
	})

	if err != nil {
		logger.Log(
			logger.Error,
			fmt.Sprintf("couldn't upload file: %s, to %s:%s", file.Name(), s3Cli.bucketName, objectKey),
			"error", err.Error(),
		)
	}

	return err
}
