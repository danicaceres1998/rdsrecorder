package aws

import (
	"context"
	"os"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
)

func CreateRDSClient(ctx context.Context, cfg awsSDK.Config) *rdsClient {
	return &rdsClient{
		baseClient: baseClient{
			ctx: ctx, cfg: cfg,
		},
	}
}

func CreateS3Client(ctx context.Context, cfg awsSDK.Config, bucketName string) *s3BucketClient {
	return &s3BucketClient{
		baseClient: baseClient{
			ctx: ctx, cfg: cfg,
		},
		bucketName: func() string {
			if envName, ok := os.LookupEnv(BucketEnvVar); ok && bucketName == "" {
				return envName
			}
			return bucketName
		}(),
	}
}
