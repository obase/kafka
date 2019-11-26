# package kafka
kafka生产者与消费者客户端.

# Installation
- go get
```
go get -u github.com/Shopify/sarama 
go get -u github.com/obase/kafka
```
- go mod
```
go mod edit -require=github.com/obase/kafka@latest
```

# Configuration
```
kafkaProducer:
  -
    key: "demo"
    address: "10.11.165.44:9092,10.11.165.44:19092,10.11.165.44:29092"
    async: false
    returnSuccess: false
    returnError: false

kafkaConsumer:
  -
    key: "demo"
    address: "10.11.165.44:9092,10.11.165.44:19092,10.11.165.44:29092"
    group: "demo"
    offset: 0
    ack: 0
    user: ""
    password: ""
    dialTimeout: "30s"
    readTimeout: "30s"
    writeTimeout: "30s"
    keepAlive: "0s"
```

# Index
- Constatns
```
const (
	ACK_BEFORE_AUTO    = 0
	ACK_AFTER_NOERROR  = 1
	ACK_AFTER_NOMATTER = 2
)
```
- Variables
```

```
- type ProducerConfig
```
type ProducerConfig struct {
	Key           string   `json:"key"`
	Address       []string `json:"address"` // kafka地址
	Async         bool     `json:"async"`
	ReturnSuccess bool     `json:"returnSuccess"`
	ReturnError   bool     `json:"returnError"`
}
```
- type ConsumerConfig
```
type ConsumerConfig struct {
	Key     string   `json:"key"`
	Address []string `json:"address"` // kafka地址
	Group   string   `json:"group"`   // groupId
	Offset  int64    `json:"offset"`
	Ack     int      `json:"ack"` // ack类型
}
```
- type ProducerMessageHandler
```
type ProducerMessageHandler func(msg *ProducerMessage)
```

- type ProducerErrorHandler 
```
type ProducerErrorHandler func(err *ProducerError)
```
- type Producer
```
type Producer interface {
	Close() error
	Produce(msgs ...*ProducerMessage) error
	AsyncHandle(mh ProducerMessageHandler, eh ProducerErrorHandler) // 必须设置 asyncReturnSuccess 或 asyncReturnError
}
```
- type ConsumerMessageHandler 
```
type ConsumerMessageHandler func(msg *ConsumerMessage) error
```

- type ConsumerErrorHandler
```
type ConsumerErrorHandler func(err error)
```
- type Consumer interface 
```
type Consumer interface {
	Close() error
	// blocking to consume the messages
	Consume(topics string, mh ConsumerMessageHandler, eh ConsumerErrorHandler) error
	ConsumeM(topics []string, mh ConsumerMessageHandler, eh ConsumerErrorHandler) error
}
```

- func GetProducer
```
func GetProducer(key string) Producer 
```
获取配置特定key的生产者

- func GetConsumer
```
func GetConsumer(key string) Consumer
```
获取配置特定key的消费者
# Examples
Consumer
```
func ListenKafkaNotify(handlers ...NotifyHandler) {

	kafkaConsumer.ConsumeM(KafkaNotifyTopic, func(msg *kafka.ConsumerMessage) (err error) {
		if notifyLogger != nil {
			notifyLogger.Info(nil, "notify received: off=%d, key=%s, val=%s", msg.Offset, msg.Key, msg.Value)
		}

		kafkaLimitChannel <- 0 // 限制最大处理数量
		go func() {
			var notify *model.NotifyMessage
			defer func() {
				_ = <-kafkaLimitChannel // 限制最大处理数量
				if perr := recover(); perr != nil {
					buf := make([]byte, 2048)
					n := runtime.Stack(buf, false)
					log.Errorf(nil, "notify process panic: message=%v, error=%v\n%s", notify, perr, buf[:n])
				}
			}()
			if err := json.Unmarshal(msg.Value, &notify); err != nil {
				log.Errorf(nil, "notify unmarshal error: %v", err)
				return
			}
			ctx, err := NewNotifyContext(notify)
			if err != nil {
				log.Errorf(nil, "notify context error: message=%v, error=%v", notify, err)
				return
			}
			defer ctx.Cleanup()

		_HANDLE:
			for i, h := range handlers {
				if ctx.abort {
					break _HANDLE
				}
				if err := h(ctx); err != nil {
					log.Errorf(nil, "notify handle error: handler=%v, message=%v, error=%v", i, notify, err)
					break _HANDLE // handler必须无错才能往下执行
				}
			}
		}()
		return
	}, errorHandler)
}
```