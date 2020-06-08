package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"strconv"
	"sync/atomic"
	"time"
)

type saramaConsumerGroupHandler struct {
	Option *ConsumerConfig
	Hander ConsumerMessageHandler
}

func newSaramaConsumerGroupHandler(mhandler ConsumerMessageHandler, option *ConsumerConfig) *saramaConsumerGroupHandler {
	return &saramaConsumerGroupHandler{
		Hander: mhandler,
		Option: option,
	}
}
func (h *saramaConsumerGroupHandler) Setup(s sarama.ConsumerGroupSession) error {
	// FIXBUG: sarama 不支持latest, ResetOffset(-1)会导致server端出现"Invalid negative offset"
	if h.Option.Offset <= 0 {
		return nil
	}
	// 如果是OffsetOldest则不变, 否则自动前移, 因为位置从0开始.
	var realOffset int64 = h.Option.Offset
	if realOffset > 0 {
		realOffset--
	}
	for t, ps := range s.Claims() {
		for _, p := range ps {
			s.ResetOffset(t, p, realOffset, "")
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
	option  *ConsumerConfig
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

func newSaramaConsumerGroup(opt *ConsumerConfig) (ret *saramaConsumerGroup, err error) {
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
