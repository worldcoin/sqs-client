## SQS Client

This library implements an SQS consumer and producer.


### Consumer
The constructor accepts the following parameters:
* `cfg`: an instance of aws sdk v2 configs
* `queueURL`: the URL of the SQS queue
* `visibilityTimeout`: visibility timeout (in seconds) applied to every message pulled from the queue
* `batchSize`: number of messages retrieved at each SQS poll
* `workersNum`: size of the workers pool
* `handler`: instance of the [Handler](./pkg/handler.go) interface that will be called for every message received

Example of usage:
```go
type MsgHandler struct {}

func (m *MsgHandler) Run(ctx context.Context, msg *Message) error {
    // Do something with msg
    return nil
}

func setupSQSConsumer(){
    ctx := context.WithCancel(context.Background())
    queueURL := "https://..."
    visibilityTimeout := 20
    batchSize := 10
    workersNum := 10
    consumer := NewConsumer(awsCfg, queueURL,  queueName, visibilityTimeout, batchSize, workersNum, MsgHandler{})
    
    go consumer.Consume(ctx)
}
```

The library supports graceful shutdown. If the caller `ctx` is cancelled, the consumer will shutdown and the 
`consumer.Consume()` method will return.

### Producer
Simple helper that sends messages to an SQS queue using the AWS SDK v2 client you pass in. It validates payloads for Standard and FIFO queues and forwards them to SQS.

Usage (Standard queue):
```go
package main

import (
    "context"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
    "github.com/worldcoin/sqs-client/producer"
)

func main() {
    ctx := context.Background()
    awsCfg, _ := config.LoadDefaultConfig(ctx)
    sqsClient := sqs.NewFromConfig(awsCfg)

    p := producer.NewProducerStandard(sqsClient, "https://sqs.us-east-1.amazonaws.com/123456789012/standard-queue")
    msg := producer.SQSMessage{messageBody: "hello world"}
    _ = p.SendMessageToQueue(ctx, msg)
}
```

Usage (FIFO queue):
```go
group := "my-group"
dedup := "my-dedup-id" // optional if content-based deduplication is enabled on the queue
p := producer.NewProducerFIFO(sqsClient, "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue.fifo")
msg := producer.SQSMessage{messageBody: "hello", messageGroupID: &group, messageDeduplicationID: &dedup}
_ = p.SendMessageToQueue(ctx, msg)
```

Notes:
- Standard queues must not set `messageGroupID` or `messageDeduplicationID`.
- FIFO queues require a non-empty `messageGroupID`.
