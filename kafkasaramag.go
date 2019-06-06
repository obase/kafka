package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"sync/atomic"
	"time"
)

type saramaConsumerGroupHandler struct {
	Offset int64
	Hander ConsumerMessageHandler
}

func newSaramaConsumerGroupHandler(mhandler ConsumerMessageHandler, offset int64) *saramaConsumerGroupHandler {
	return &saramaConsumerGroupHandler{
		Hander: mhandler,
		Offset: offset,
	}
}
func (h *saramaConsumerGroupHandler) Setup(s sarama.ConsumerGroupSession) error {
	if h.Offset != 0 {
		for t, ps := range s.Claims() {
			for _, p := range ps {
				s.ResetOffset(t, p, h.Offset, "")
			}
		}
	}
	return nil
}
func (h *saramaConsumerGroupHandler) Cleanup(s sarama.ConsumerGroupSession) error { return nil }
func (h *saramaConsumerGroupHandler) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {
	for msg := range c.Messages() {
		h.Hander(msg)
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
	return g.ConsumerGroup.Close()
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
	for {
		err = g.ConsumerGroup.Consume(context.Background(), []string{topic}, newSaramaConsumerGroupHandler(mh, g.option.Offset))
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
