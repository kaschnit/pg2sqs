package publish

import "github.com/aws/aws-sdk-go-v2/service/sqs"

var _ Sender = (*sqs.Client)(nil)
