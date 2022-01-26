package sqs

import (
	"github.com/aws/aws-sdk-go/aws/client"
)

type retryer struct {
	client.DefaultRetryer
	retryCount int
}

// MaxRetries sets the total exponential back off attempts to 10 retries
func (r retryer) MaxRetries() int {
	if r.retryCount > 0 {
		return r.retryCount
	}

	return 10
}
