package kafka

import (
	"github.com/Shopify/sarama"
)

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
}

type ProducerMessageHandler func(msg *sarama.ProducerMessage)

type ProducerErrorHandler func(err *sarama.ProducerError)

type Producer interface {
	Close() error
	Produce(msgs ...*sarama.ProducerMessage) error
	AsyncHandle(mh ProducerMessageHandler, eh ProducerErrorHandler) // 必须设置 asyncReturnSuccess 或 asyncReturnError
}

type ConsumerMessageHandler func(msg *sarama.ConsumerMessage)

type ConsumerErrorHandler func(err *sarama.ConsumerError)

type Consumer interface {
	Close() error
	// blocking to consume the messages
	Consume(topics []string, mh ConsumerMessageHandler, eh ConsumerErrorHandler) error
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
