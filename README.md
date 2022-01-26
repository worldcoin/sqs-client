### SQS Client

This library implements a SQS consumer able to retry messages and extend their visibility timeout.

The library exposes a configuration struct ([sqs.Config](./pkg/consumer.go)) that can be used to setup the consumer correctly.

In order to use the library in tests, the `DisableSSL` config needs to be set to true and the correct `QueueHost` needs to be passed.
The value of the host could depend on the environment where the tests run (e.g. local or CI).

The library accepts an instance of [Handler](./pkg/handler.go) that will be called to handle incoming messages.

Example of usage:
```go

conf := Config{
    AwsAccessKey:      "sqs",
    AwsSecretKey:      "sqs",
    AwsRegion:         "us-east-1",
    QueueHost:         "http://localhost:9324",
    WorkerPool:        1,
    VisibilityTimeout: 15,
    DisableSSL:        true,
}

queueName := "test_queue"

queue, err := CreateQueue(conf, queueName)
if err != nil {
    log.Error("error while creating queue")
    t.FailNow()
}

consumer, err := NewConsumer(conf, queueName)
if err != nil {
    log.Error("error while creating consumer")
    t.FailNow()
}

handler := func(ctx context.Context, m *Message) error {
    // Handle message
    return nil
}

consumer.RegisterHandler(handler)
go consumer.Consume()
```