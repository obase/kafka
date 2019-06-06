package kafka

import (
	"github.com/Shopify/sarama"
	"sync/atomic"
	"time"
)

type saramaConsumer struct {
	sarama.Consumer
	option  *ConsumerOption
	version int32
}

func (g *saramaConsumer) Close() error {
	return g.Close()
}

// blocking to consume the messages
func (g *saramaConsumer) Consume(topic string, mh ConsumerMessageHandler, eh ConsumerErrorHandler) (err error) {

	parts, err := g.Partitions(topic)
	if err != nil {
		return
	}
	var pcs []sarama.PartitionConsumer
	defer func() {
		for _, pc := range pcs {
			pc.Close()
		}
	}()
	var pc sarama.PartitionConsumer
	for _, part := range parts {
		pc, err = g.ConsumePartition(topic, part, g.option.Offset)
		if err != nil {
			return
		}
		pcs = append(pcs, pc)
		ver := atomic.AddInt32(&g.version, 1)
		// 错误处理
		go func() {
			tk := time.Tick(time.Second)
			for ver == g.version {
				select {
				case e := <-pc.Errors():
					eh(e)
				case <-tk:
				}
			}
		}()
		// 消息处理
		go func() {
			tk := time.Tick(time.Second)
			for ver == g.version {
				select {
				case m := <-pc.Messages():
					mh(m)
				case <-tk:
				}
			}
		}()
	}
	return
}

func newSaramaConsumer(opt *ConsumerOption) (ret *saramaConsumer, err error) {
	csr, err := sarama.NewConsumer(opt.Address, consumerConfig(opt))
	if err != nil {
		return
	}
	ret = &saramaConsumer{
		Consumer: csr,
		option:   opt,
	}
	return
}
