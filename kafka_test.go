package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"strconv"
	"testing"
	"time"
)

func TestGetProducer(t *testing.T) {
	Init()
	p := GetProducer("demo")
	defer p.Close()
	for i := 0; i < 10000; i++ {
		p.Produce(&ProducerMessage{
			Topic: "test.topic",
			Key:   sarama.StringEncoder("this is a key " + strconv.Itoa(i)),
			Value: sarama.StringEncoder("this is a value " + strconv.Itoa(i)),
		})
		fmt.Println("produce " + strconv.Itoa(i))
		time.Sleep(time.Second)
	}
}

func TestGetConsumer(t *testing.T) {
	Init()
	c := GetConsumer("demo")
	defer c.Close()
	c.Consume("test.topic", func(msg *ConsumerMessage) {
		fmt.Printf("receive off=%v, key=%v, val=%v\n", msg.Offset, msg.Key, msg.Value)
	}, func(err error) {
		fmt.Printf("error: %v\n", err)
	})
}
