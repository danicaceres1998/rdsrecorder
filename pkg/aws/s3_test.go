package aws

import (
	"errors"
	"fmt"
	"os"
	helper "rdsrecorder/pkg/processhelper"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestVerifyBucket(t *testing.T) {
	data := []struct {
		name       string
		expected   bool
		bucketName string
		err        error
	}{
		{"bucket-exists", true, "test-bucket", nil},
		{"bucket-not-exists", false, "non-exists-bucket", nil},
		{"bucket-list-error", false, "bucket-error", errors.New("list-error")},
	}
	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			clientMock := createS3ClientMock(func() string {
				if d.expected {
					return d.bucketName
				}
				return ""
			}())
			clientMock.On("ListBuckets", mock.Anything).Return(
				&s3.ListBucketsOutput{
					Buckets: []types.Bucket{{Name: &d.bucketName}},
				},
				d.err,
			)

			result := VerifyBucket(clientMock)
			assert.Equal(t, d.expected, result)
		})
	}
}

func TestVerifyBucketFolder(t *testing.T) {
	data := []struct {
		name     string
		expected bool
		cached   bool
		listErr  error
	}{
		{"folder-exists", true, false, nil},
		{"folder-not-exists", true, false, nil},
		{"cached-exists", true, true, nil},
		{"list-error", false, false, errors.New("unable to list objects")},
	}
	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			folderExists = d.cached
			clientMock := createS3ClientMock()
			clientMock.On("ListObjectsV2", mock.Anything).Return(
				&s3.ListObjectsV2Output{
					Contents: func() []types.Object {
						if d.expected {
							return []types.Object{{}}
						}
						return []types.Object{}
					}(),
				},
				d.listErr,
			)

			result := verifyBucketFolder(clientMock, "test-folder")
			assert.Equal(t, d.expected, result)
			if !d.cached {
				clientMock.AssertCalled(t, "ListObjectsV2")
			}
		})
	}
}

func TestCreateBucketFolder(t *testing.T) {
	data := []struct {
		name     string
		expected error
	}{
		{"success-creation", nil},
		{"error-on-creation", errors.New("unable to create the folder")},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			clientMock := createS3ClientMock()
			clientMock.On("PutObject", mock.Anything).Return(&s3.PutObjectOutput{}, d.expected)

			err := createBucketFolder(clientMock, "folder", "test-db")
			assert.Equal(t, d.expected, err)
		})
	}
}

func TestFormatFilePath(t *testing.T) {
	path, filename := "folder", "filename"
	assert.Equal(t,
		fmt.Sprintf("%s/%s", path, filename),
		formatFilePath(path, filename),
	)
}

func TestPushLogToBucket(t *testing.T) {
	data := []struct {
		name        string
		expected    error
		folderExist bool  // ListObjectsV2
		putError    error // PutObject
	}{
		{
			"success-upload/existing-folder", nil,
			true, nil,
		},
		{
			"success-upload/unexisting-folder", nil,
			false, nil,
		},
		{
			"error-upload/uanble-create-folder", errors.New("unable to create the folder"),
			false, errors.New("unable to create the folder"),
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			folderExists = false
			var file *os.File
			if d.expected == nil {
				file, _ = os.CreateTemp("/var/tmp", "s3-test-file-")
			}
			clientMock := createS3ClientMock()
			clientMock.On("ListObjectsV2", mock.Anything).Return(
				&s3.ListObjectsV2Output{
					Contents: func() []types.Object {
						if d.folderExist {
							return []types.Object{{}}
						}
						return []types.Object{}
					}(),
				},
				nil,
			)
			if !d.folderExist {
				clientMock.On("PutObject", mock.Anything).Return(&s3.PutObjectOutput{}, d.putError)
			}
			clientMock.On("UploadLargeFile", mock.Anything).Return(d.expected)

			result := PushLogToBucket(clientMock, file, "test-file-upload", "test-db")
			if d.expected == nil {
				assert.Nil(t, result)
			} else {
				assert.Error(t, result)
			}

			clientMock.AssertCalled(t, "ListObjectsV2")
			if !d.folderExist {
				clientMock.AssertCalled(t, "PutObject")
			}
			if d.putError == nil {
				clientMock.AssertCalled(t, "UploadLargeFile")
			}

			helper.CleanTmpFile(file)
		})
	}
}
