package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestGetProducer(t *testing.T) {
	p := GetProducer("demo")
	defer p.Close()
	for i := 0; i < 10000; i++ {
		msg := &ProducerMessage{
			Topic:  "test.topic",
			Key:    sarama.StringEncoder("this is a key " + strconv.Itoa(i)),
			Value:  sarama.StringEncoder("this is a value " + strconv.Itoa(i)),
			Offset: int64(i),
		}
		p.Produce(msg)
		fmt.Println("produce " + strconv.Itoa(i) + ", off=" + strconv.FormatInt(msg.Offset, 10))
		time.Sleep(3 * time.Second)
	}
}

func TestGetConsumer(t *testing.T) {
	c := GetConsumer("demo")
	defer c.Close()
	c.Consume("test.topic", func(msg *ConsumerMessage) error {
		fmt.Printf("receive off=%v, key=%v, val=%v\n", msg.Offset, string(msg.Key), string(msg.Value))
		os.Exit(1)
		return nil
	}, func(err error) {
		fmt.Printf("error: %v\n", err)
	})
}
