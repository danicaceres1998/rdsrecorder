package aws

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigWithRetryer(t *testing.T) {
	cfg, err := configWithRetryer(context.Background())

	assert.Nil(t, err)
	assert.Equal(t, defaultRegion, cfg.Region)
	assert.Equal(t, RetriesToManyRequests, cfg.Retryer().MaxAttempts())
}

func TestLoadRegion(t *testing.T) {
	data := []struct {
		name     string
		expected string
		region   string
	}{
		{"env region", "us-east-1", "us-east-1"},
		{"default region", defaultRegion, ""},
	}

	for _, d := range data {
		_ = os.Setenv(regionEnvKey, d.region)
		assert.Equal(t, d.expected, loadRegion())
	}
}
