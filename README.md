### SQS Client

This library implements a SQS consumer.

The constructor accepts the following parameters:
* `cfg`: an instance of aws sdk v2 configs
* `queueURL`: the URL of the SQS queue
* `visibilityTimeout`: visibility timeout (in seconds) applied to every message pulled from the queue
* `batchSize`: number of messages retrieved at each SQS poll
* `workersNum`: size of the workers pool
* `handler`: instance of the [Handler](./handler.go) interface that will be called for every message received

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