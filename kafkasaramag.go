package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"sync/atomic"
	"time"
)

func (h ConsumerMessageHandler) Setup(s sarama.ConsumerGroupSession) error   { return nil }
func (h ConsumerMessageHandler) Cleanup(s sarama.ConsumerGroupSession) error { return nil }
func (h ConsumerMessageHandler) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {
	for msg := range c.Messages() {
		h(msg)
		s.MarkMessage(msg, "")
	}
	return nil
}

type saramaConsumerGroup struct {
	sarama.ConsumerGroup
	option  *ConsumerOption
	version int32
}

func (g *saramaConsumerGroup) Close() error {
	return g.Close()
}

// blocking to consume the messages
func (g *saramaConsumerGroup) Consume(topic string, mh ConsumerMessageHandler, eh ConsumerErrorHandler) (err error) {
	ver := atomic.AddInt32(&g.version, 1)
	go func() {
		tk := time.Tick(time.Second)
		for ver == g.version {
			select {
			case e := <-g.Errors():
				eh(e)
			case <-tk:
			}
		}
	}()
	ctx := context.Background()
	for {
		err = g.ConsumerGroup.Consume(ctx, []string{topic}, mh)
		if err != nil {
			return
		}
	}

	return nil
}

func newSaramaConsumerGroup(opt *ConsumerOption) (ret *saramaConsumerGroup, err error) {
	grp, err := sarama.NewConsumerGroup(opt.Address, opt.Group, consumerConfig(opt))
	if err != nil {
		return
	}
	ret = &saramaConsumerGroup{
		ConsumerGroup: grp,
		option:        opt,
	}
	return
}
