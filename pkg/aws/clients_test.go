package aws

import (
	"context"
	"os"
	"testing"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
)

func TestCreateRDSClient(t *testing.T) {
	ctx, cfg := context.Background(), awsSDK.NewConfig()
	client := CreateRDSClient(ctx, *cfg)

	assert.Equal(t, ctx, client.GetContext())
	assert.Equal(t, *cfg, client.GetConfig())
}

func TestCreateS3Client(t *testing.T) {
	ctx, cfg, bucketName := context.Background(), awsSDK.NewConfig(), "test-bucket"
	client := CreateS3Client(ctx, *cfg, bucketName)

	assert.Equal(t, ctx, client.GetContext())
	assert.Equal(t, *cfg, client.GetConfig())
	assert.Equal(t, bucketName, client.bucketName)

	// ENV bucket name
	bucketName = "env-bucket-name"
	err := os.Setenv(BucketEnvVar, bucketName)
	assert.Nil(t, err)
	assert.Equal(t,
		bucketName,
		CreateS3Client(ctx, *cfg, "").bucketName,
	)
}
