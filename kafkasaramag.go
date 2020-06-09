package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"strconv"
)

type saramaConsumerGroupHandler struct {
	*ConsumerConfig
	ConsumerMessageHandler
	context.Context
}

func (h *saramaConsumerGroupHandler) Setup(s sarama.ConsumerGroupSession) error {
	// FIXBUG: sarama 不支持latest, ResetOffset(-1)会导致server端出现"Error: Executing consumer group command failed due to java.lang.IllegalArgumentException: Invalid negative offset"
	if h.ConsumerConfig.Offset > 0 || h.ConsumerConfig.Offset == sarama.OffsetOldest {
		// 如果是OffsetOldest则不变, 否则自动前移, 因为位置从0开始.
		var realOffset int64 = h.ConsumerConfig.Offset
		if realOffset > 0 {
			realOffset--
		}
		for t, ps := range s.Claims() {
			for _, p := range ps {
				s.ResetOffset(t, p, realOffset, "")
			}
		}
	}
	return nil
}
func (h *saramaConsumerGroupHandler) Cleanup(s sarama.ConsumerGroupSession) error { return nil }
func (h *saramaConsumerGroupHandler) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) (err error) {
	// 使用context控制message与error的handler的退出时机
	for {
		select {
		case <-h.Context.Done():
			return
		case msg := <-c.Messages():
			if msg != nil {
				switch h.ConsumerConfig.Ack {
				case ACK_BEFORE_AUTO:
					s.MarkMessage(msg, "")
					err = h.ConsumerMessageHandler(msg)
				case ACK_AFTER_NOERROR:
					if err = h.ConsumerMessageHandler(msg); err == nil {
						s.MarkMessage(msg, "")
					}
				case ACK_AFTER_NOMATTER:
					err = h.ConsumerMessageHandler(msg)
					s.MarkMessage(msg, "")
				default:
					panic("invalid ack type: " + strconv.Itoa(h.ConsumerConfig.Ack))
				}
			}
		}
	}

}

type saramaConsumerGroup struct {
	*ConsumerConfig
	sarama.ConsumerGroup
	context.CancelFunc
}

func newSaramaConsumerGroup(c *ConsumerConfig) (ret *saramaConsumerGroup, err error) {
	grp, err := sarama.NewConsumerGroup(c.Address, c.Group, consumerConfig(c))
	if err != nil {
		return
	}
	ret = &saramaConsumerGroup{
		ConsumerConfig: c,
		ConsumerGroup:  grp,
	}
	return
}

func (g *saramaConsumerGroup) Close() (err error) {
	if g.CancelFunc != nil {
		g.CancelFunc()
	}
	if g.ConsumerGroup != nil {
		err = g.ConsumerGroup.Close()
	}
	return
}

// 必须保证ConsumerMessageHandler, ConsumerErrorHandler没有panic
func (g *saramaConsumerGroup) Consume(topic string, mh ConsumerMessageHandler, eh ConsumerErrorHandler) (err error) {
	return g.ConsumeM([]string{topic}, mh, eh)
}

// 必须保证ConsumerMessageHandler, ConsumerErrorHandler没有panic
func (g *saramaConsumerGroup) ConsumeM(topics []string, mh ConsumerMessageHandler, eh ConsumerErrorHandler) (err error) {
	// 先关闭旧的消费过程
	if g.CancelFunc != nil {
		g.CancelFunc()
	}

	// 每次进入都会新起context. 因为ConsumeM()不能重复调用,否则会停掉之前的工作
	var ctx context.Context
	ctx, g.CancelFunc = context.WithCancel(context.Background())
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-g.Errors():
				eh(e)
			}
		}
	}(ctx)
	for {
		err = g.ConsumerGroup.Consume(ctx, topics, &saramaConsumerGroupHandler{
			ConsumerConfig:         g.ConsumerConfig,
			ConsumerMessageHandler: mh,
			Context:                ctx,
		})
		if err != nil {
			return
		} else if err = ctx.Err(); err != nil {
			return
		}
	}
	return
}
