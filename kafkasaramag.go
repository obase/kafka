package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"strconv"
	"sync/atomic"
	"time"
)

type saramaConsumerGroupHandler struct {
	Option *ConsumerOption
	Hander ConsumerMessageHandler
}

func newSaramaConsumerGroupHandler(mhandler ConsumerMessageHandler, option *ConsumerOption) *saramaConsumerGroupHandler {
	return &saramaConsumerGroupHandler{
		Hander: mhandler,
		Option: option,
	}
}
func (h *saramaConsumerGroupHandler) Setup(s sarama.ConsumerGroupSession) error {
	if h.Option.Offset != 0 {
		for t, ps := range s.Claims() {
			for _, p := range ps {
				s.ResetOffset(t, p, h.Option.Offset, "")
			}
		}
	}
	return nil
}
func (h *saramaConsumerGroupHandler) Cleanup(s sarama.ConsumerGroupSession) error { return nil }
func (h *saramaConsumerGroupHandler) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) (err error) {
	for msg := range c.Messages() {
		switch h.Option.Ack {
		case ACK_BEFORE_AUTO:
			s.MarkMessage(msg, "")
			err = h.Hander(msg)
		case ACK_AFTER_NOERROR:
			if err = h.Hander(msg); err == nil {
				s.MarkMessage(msg, "")
			}
		case ACK_AFTER_NOMATTER:
			err = h.Hander(msg)
			s.MarkMessage(msg, "")
		default:
			panic("invalid ack type: " + strconv.Itoa(h.Option.Ack))
		}
	}
	return
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
	return g.ConsumeM([]string{topic}, mh, eh)
}

// blocking to consume the messages
func (g *saramaConsumerGroup) ConsumeM(topics []string, mh ConsumerMessageHandler, eh ConsumerErrorHandler) (err error) {
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
		err = g.ConsumerGroup.Consume(context.Background(), topics, newSaramaConsumerGroupHandler(mh, g.option))
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
