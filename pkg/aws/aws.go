package aws

import (
	"context"
	"os"

	awsSDK "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

const (
	RetriesToManyRequests = 200
	defaultRegion         = "sa-east-1"
	regionEnvKey          = "AWS_REGION"
)

func VerifyAWSConfig(ctx context.Context) (awsSDK.Config, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(loadRegion()))
	if err != nil {
		return awsSDK.Config{}, err
	}

	// Checking configuration
	svc := sts.NewFromConfig(cfg)
	if _, err = svc.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}); err != nil {
		return awsSDK.Config{}, err
	}

	return configWithRetryer(ctx)
}

func configWithRetryer(ctx context.Context) (awsSDK.Config, error) {
	return config.LoadDefaultConfig(
		ctx, config.WithRegion(loadRegion()),
		config.WithRetryer(func() awsSDK.Retryer {
			return retry.AddWithMaxAttempts(
				retry.NewStandard(
					func(so *retry.StandardOptions) {
						so.Retryables = []retry.IsErrorRetryable{
							retry.RetryableConnectionError{},
							retry.RetryableHTTPStatusCode{
								Codes: retry.DefaultRetryableHTTPStatusCodes,
							},
							retry.RetryableErrorCode{
								Codes: retry.DefaultRetryableErrorCodes,
							},
							retry.RetryableErrorCode{
								Codes: retry.DefaultThrottleErrorCodes,
							},
						}
					},
				),
				RetriesToManyRequests,
			)
		}),
	)
}

func loadRegion() string {
	if region, ok := os.LookupEnv(regionEnvKey); ok && region != "" {
		return region
	}

	return defaultRegion
}
