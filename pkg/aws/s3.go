package aws

import (
	"fmt"
	"os"
	"rdsrecorder/pkg/logger"
	pHelper "rdsrecorder/pkg/processhelper"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	BucketEnvVar = "AWS_S3_BUCKET_NAME"
	folderExists = false // ONLY CHANGE THIS ON verifyBucketFolder()
)

func VerifyBucket(client S3BucketClient) bool {
	buckets, err := client.ListBuckets(nil)
	if err != nil {
		logger.Log(logger.Error, "unable to get buckets", "error", err.Error())
		return false
	}

	for _, b := range buckets.Buckets {
		if client.GetBucketName() == *b.Name {
			return true
		}
	}
	return false
}

func PushLogToBucket(client S3BucketClient, targetFile *os.File, fileName, dbIdentifier string) error {
	folder := pHelper.GetProcessID(client.GetContext())

	if !verifyBucketFolder(client, folder) {
		if err := createBucketFolder(client, folder, dbIdentifier); err != nil {
			return err
		}
		logger.Log(logger.Info, "the S3 bucket folder is created")
	}

	return client.UploadLargeFile(targetFile, formatFilePath(folder, fileName))
}

// Private Functions //

func verifyBucketFolder(client S3BucketClient, folder string) bool {
	if folderExists {
		return folderExists
	}

	folderExists = func() bool {
		r, err := client.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:  awsSDK.String(client.GetBucketName()),
			Prefix:  awsSDK.String(folder + "/"),
			MaxKeys: awsSDK.Int32(1),
		})
		if err != nil {
			logger.Log(logger.Error, fmt.Sprintf("couldn't get the object: %s", folder), "error", err.Error())
			return false
		}
		if len(r.Contents) == 0 {
			return false
		}

		return true
	}()

	return folderExists
}

func createBucketFolder(client S3BucketClient, folder, dbIdentifier string) error {
	_, err := client.PutObject(&s3.PutObjectInput{
		Bucket: awsSDK.String(client.GetBucketName()),
		Key:    awsSDK.String(folder + "/"),
		Metadata: map[string]string{
			"db-identifier": dbIdentifier,
		},
	})

	return err
}

func formatFilePath(path, filename string) string {
	return fmt.Sprintf("%s/%s", path, filename)
}
