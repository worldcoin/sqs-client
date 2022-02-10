### SQS Client

This library implements a SQS consumer.

The library accepts the following parameters in the constructor:
* `ctx`: the context of the caller
* `cfg`: an instance of aws sdk v2 configs
* `queueName`: the SQS queue name
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
    visibilityTimeout := 20
    batchSize := 10
    workersNum := 10
    consumer, err := NewConsumer(ctx, awsCfg, queueName, visibilityTimeout, batchSize, workersNum, MsgHandler{})
    if err != nil {
        log.Error("error while creating consumer")
    }
    
    go consumer.Consume()
}
```