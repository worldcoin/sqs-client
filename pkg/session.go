package sqs

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
)

func newSession(c Config) (*session.Session, error) {
	creds := credentials.NewStaticCredentials(c.AwsAccessKey, c.AwsSecretKey, "")
	_, err := creds.Get()
	if err != nil {
		return nil, fmt.Errorf("invalid credentials: %w", err)
	}

	r := &retryer{retryCount: 10}

	cfg := request.WithRetryer(
		aws.
			NewConfig().
			WithRegion(c.AwsRegion).
			WithCredentials(creds).
			WithDisableSSL(c.DisableSSL), r)

	if c.QueueHost != "" {
		cfg.Endpoint = &c.QueueHost
	}

	return session.NewSession(cfg)
}
