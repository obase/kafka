package kafka

import (
	"github.com/Shopify/sarama"
)

type ProducerMessage = sarama.ProducerMessage
type ProducerError = sarama.ProducerError
type ConsumerMessage = sarama.ConsumerMessage
type ConsumerError = sarama.ConsumerError

type ProducerOption struct {
	Key                string   `json:"key"`
	Address            []string `json:"address"` // kafka地址
	Async              bool     `json:"async"`
	AsyncReturnSuccess bool     `json:"asyncReturnSuccess"`
	AsyncReturnError   bool     `json:"asyncReturnError"`
}

type ConsumerOption struct {
	Key     string   `json:"key"`
	Address []string `json:"address"` // kafka地址
	Group   string   `json:"group"`   // groupId
	Offset  int64
}

type ProducerMessageHandler func(msg *ProducerMessage)

type ProducerErrorHandler func(err *ProducerError)

type Producer interface {
	Close() error
	Produce(msgs ...*ProducerMessage) error
	AsyncHandle(mh ProducerMessageHandler, eh ProducerErrorHandler) // 必须设置 asyncReturnSuccess 或 asyncReturnError
}

type ConsumerMessageHandler func(msg *ConsumerMessage)

type ConsumerErrorHandler func(err error)

type Consumer interface {
	Close() error
	// blocking to consume the messages
	Consume(topics string, mh ConsumerMessageHandler, eh ConsumerErrorHandler) error
}

func producerConfig(opt *ProducerOption) (config *sarama.Config) {
	config = sarama.NewConfig()
	config.Producer.Return.Successes = opt.AsyncReturnSuccess
	config.Producer.Return.Errors = opt.AsyncReturnError
	return
}

func consumerConfig(opt *ConsumerOption) (config *sarama.Config) {
	config = sarama.NewConfig()
	return
}
