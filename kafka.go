package kafka

import (
	"github.com/Shopify/sarama"
	"strings"
)

const (
	ACK_BEFORE_AUTO    = 0
	ACK_AFTER_NOERROR  = 1
	ACK_AFTER_NOMATTER = 2
)

type ProducerMessage = sarama.ProducerMessage
type ProducerError = sarama.ProducerError
type ConsumerMessage = sarama.ConsumerMessage
type ConsumerError = sarama.ConsumerError

type ProducerConfig struct {
	Key           string   `json:"key"`
	Address       []string `json:"address"` // kafka地址
	Async         bool     `json:"async"`
	ReturnSuccess bool     `json:"returnSuccess"`
	ReturnError   bool     `json:"returnError"`
}

type ConsumerConfig struct {
	Key     string   `json:"key"`
	Address []string `json:"address"` // kafka地址
	Group   string   `json:"group"`   // groupId
	Offset  int64    `json:"offset"`
	Ack     int      `json:"ack"` // ack类型
}

type ProducerMessageHandler func(msg *ProducerMessage)

type ProducerErrorHandler func(err *ProducerError)

type Producer interface {
	Close() error
	Produce(msgs ...*ProducerMessage) error
	AsyncHandle(mh ProducerMessageHandler, eh ProducerErrorHandler) // 必须设置 asyncReturnSuccess 或 asyncReturnError
}

type ConsumerMessageHandler func(msg *ConsumerMessage) error

type ConsumerErrorHandler func(err error)

type Consumer interface {
	Close() error
	// blocking to consume the messages
	Consume(topics string, mh ConsumerMessageHandler, eh ConsumerErrorHandler) error
	ConsumeM(topics []string, mh ConsumerMessageHandler, eh ConsumerErrorHandler) error
}

func producerConfig(opt *ProducerConfig) (config *sarama.Config) {
	config = sarama.NewConfig()
	config.Version = sarama.V0_10_2_0 // consumer groups require Version to be >= V0_10_2_0
	config.Producer.Return.Successes = opt.ReturnSuccess
	config.Producer.Return.Errors = opt.ReturnError
	return
}

func consumerConfig(opt *ConsumerConfig) (config *sarama.Config) {
	config = sarama.NewConfig()
	config.Version = sarama.V0_10_2_0 // consumer groups require Version to be >= V0_10_2_0
	return
}

var producers map[string]Producer = make(map[string]Producer)
var consumers map[string]Consumer = make(map[string]Consumer)

func GetProducer(key string) Producer {
	if rt, ok := producers[key]; ok {
		return rt
	}
	return nil
}

func GetConsumer(key string) Consumer {
	if rt, ok := consumers[key]; ok {
		return rt
	}
	return nil
}

func SetupProducer(opt *ProducerConfig) (err error) {
	var p Producer
	if opt.Async {
		p, err = newSaramaAsyncProducer(opt)
	} else {
		p, err = newSaramaSyncProducer(opt)
	}
	if err != nil {
		return
	}
	for _, k := range strings.Split(opt.Key, ",") {
		if k = strings.TrimSpace(k); len(k) > 0 {
			producers[k] = p
		}
	}

	return
}

func SetupConsumer(opt *ConsumerConfig) (err error) {
	var c Consumer
	c, err = newSaramaConsumerGroup(opt)
	if err != nil {
		return
	}

	for _, k := range strings.Split(opt.Key, ",") {
		if k = strings.TrimSpace(k); len(k) > 0 {
			consumers[k] = c
		}
	}
	return
}

func Close() {
	for _, p := range producers {
		p.Close()
	}
	for _, c := range consumers {
		c.Close()
	}
}
