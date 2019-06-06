package kafka

import (
	"github.com/Shopify/sarama"
	"sync/atomic"
	"time"
)

/*=====================================
sync producer
 ======================================*/
type saramaSyncProducer struct {
	sarama.SyncProducer
	option *ProducerOption
}

func (p *saramaSyncProducer) Close() error {
	return p.Close()
}

func (p *saramaSyncProducer) Produce(msgs ...*sarama.ProducerMessage) error {
	return p.SendMessages(msgs)
}

func (p *saramaSyncProducer) AsyncHandle(mh ProducerMessageHandler, eh ProducerErrorHandler) {

}

func newSaramaSyncProducer(opt *ProducerOption) (ret *saramaSyncProducer, err error) {
	// must be set true to return.errors
	opt.ReturnError = true
	p, err := sarama.NewSyncProducer(opt.Address, producerConfig(opt))
	if err != nil {
		return
	}
	ret = &saramaSyncProducer{
		option:       opt,
		SyncProducer: p,
	}
	return
}

/*=====================================
async producer
 ======================================*/
type saramaAsyncProducer struct {
	sarama.AsyncProducer
	option  *ProducerOption
	version int32
}

func (p *saramaAsyncProducer) Close() error {
	return p.Close()
}

func (p *saramaAsyncProducer) Produce(msgs ...*sarama.ProducerMessage) error {
	for _, m := range msgs {
		p.Input() <- m
	}
	return nil
}

func (p *saramaAsyncProducer) AsyncHandle(mh ProducerMessageHandler, eh ProducerErrorHandler) {
	ver := atomic.AddInt32(&p.version, 1)
	if p.option.ReturnSuccess && mh != nil {
		go func() {
			tk := time.Tick(time.Second)
			for p.version == ver {
				mh(<-p.Successes())
				select {
				case m := <-p.Successes():
					mh(m)
				case <-tk:
				}
			}
		}()
	}
	if p.option.ReturnError && eh != nil {
		go func() {
			tk := time.Tick(time.Second)
			for p.version == ver {
				select {
				case e := <-p.Errors():
					eh(e)
				case <-tk:
				}
			}
		}()
	}
}

func newSaramaAsyncProducer(opt *ProducerOption) (ret *saramaAsyncProducer, err error) {
	p, err := sarama.NewAsyncProducer(opt.Address, producerConfig(opt))
	if err != nil {
		return
	}
	ret = &saramaAsyncProducer{
		option:        opt,
		AsyncProducer: p,
	}
	// 默认异步读取,避免阻塞
	if opt.ReturnSuccess {
		go func() {
			tk := time.Tick(time.Second)
			for ret.version == 0 {
				select {
				case <-ret.Successes():
				case <-tk:
				}
			}
		}()
	}
	// 默认异步读取,避免阻塞
	if opt.ReturnError {
		go func() {
			tk := time.Tick(time.Second)
			for ret.version == 0 {
				select {
				case <-ret.Errors():
				case <-tk:
				}
			}
		}()
	}
	return
}
