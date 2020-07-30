package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/obase/conf"
	"strconv"
	"strings"
)

const PCKEY = "kafkaProducer"
const CCKEY = "kafkaConsumer"

func init() {
	configs, ok := conf.GetSlice(PCKEY)
	if ok && len(configs) > 0 {
		for i, c := range configs {
			key, ok := conf.ElemString(c, "key")
			if !ok {
				Close()
				panic("Undef kafka producder key of " + strconv.Itoa(i))
			}
			address, ok := conf.ElemStringSlice(c, "address")
			async, ok := conf.ElemBool(c, "async")
			returnSuccess, ok := conf.ElemBool(c, "returnSuccess")
			returnError, ok := conf.ElemBool(c, "returnError")
			version, ok := conf.ElemString(c, "version")

			err := SetupProducer(&ProducerConfig{
				Key:           key,
				Address:       address,
				Async:         async,
				ReturnSuccess: returnSuccess,
				ReturnError:   returnError,
				Version:       getKafkaVersion(version),
			})
			if err != nil {
				Close()
				panic(fmt.Sprintf("Setup kafka consumer failed, key=%v, err=%v ", key, err))
			}
		}
	}
	configs, ok = conf.GetSlice(CCKEY)
	if ok && len(configs) > 0 {
		for i, c := range configs {
			key, ok := conf.ElemString(c, "key")
			if !ok {
				Close()
				panic("Undef kafka consumer key " + strconv.Itoa(i))
			}
			address, ok := conf.ElemStringSlice(c, "address")
			group, ok := conf.ElemString(c, "group")
			if !ok {
				Close()
				panic("Undef kafka consumer group " + strconv.Itoa(i))
			}
			offset, ok := conf.ElemInt(c, "offset")
			ack, ok := conf.ElemInt(c, "ack")
			user, ok := conf.ElemString(c, "user")
			password, ok := conf.ElemString(c, "password")

			dialTimeout, ok := conf.ElemDuration(c, "dialTimeout")
			readTimeout, ok := conf.ElemDuration(c, "readTimeout")
			writeTimeout, ok := conf.ElemDuration(c, "writeTimeout")
			keepAlive, ok := conf.ElemDuration(c, "keepAlive")
			version, ok := conf.ElemString(c, "version")

			err := SetupConsumer(&ConsumerConfig{
				Key:          key,
				Address:      address,
				Group:        group,
				Offset:       int64(offset),
				Ack:          ack,
				User:         user,
				Password:     password,
				DialTimeout:  dialTimeout,
				ReadTimeout:  readTimeout,
				WriteTimeout: writeTimeout,
				KeepAlive:    keepAlive,
				Version:      getKafkaVersion(version),
			})
			if err != nil {
				Close()
				panic(fmt.Sprintf("Setup kafka consumer failed, key=%v, err=%v ", key, err))
			}
		}
	}
}

func getKafkaVersion(version string) *sarama.KafkaVersion {
	switch strings.ToUpper(version) {
	case "":
		return nil
	case "V0_8_2_0":
		return &sarama.V0_8_2_0
	case "V0_8_2_1":
		return &sarama.V0_8_2_1
	case "V0_8_2_2":
		return &sarama.V0_8_2_2
	case "V0_9_0_0":
		return &sarama.V0_9_0_0
	case "V0_9_0_1":
		return &sarama.V0_9_0_1
	case "V0_10_0_0":
		return &sarama.V0_10_0_0
	case "V0_10_0_1":
		return &sarama.V0_10_0_1
	case "V0_10_1_0":
		return &sarama.V0_10_1_0
	case "V0_10_1_1":
		return &sarama.V0_10_1_1
	case "V0_10_2_0":
		return &sarama.V0_10_2_0
	case "V0_10_2_1":
		return &sarama.V0_10_2_1
	case "V0_11_0_0":
		return &sarama.V0_11_0_0
	case "V0_11_0_1":
		return &sarama.V0_11_0_1
	case "V0_11_0_2":
		return &sarama.V0_11_0_2
	case "V1_0_0_0":
		return &sarama.V1_0_0_0
	case "V1_1_0_0":
		return &sarama.V1_1_0_0
	case "V1_1_1_0":
		return &sarama.V1_1_1_0
	case "V2_0_0_0":
		return &sarama.V2_0_0_0
	case "V2_0_1_0":
		return &sarama.V2_0_1_0
	case "V2_1_0_0":
		return &sarama.V2_1_0_0
	case "V2_2_0_0":
		return &sarama.V2_2_0_0
	case "V2_3_0_0":
		return &sarama.V2_3_0_0
	case "V2_4_0_0":
		return &sarama.V2_4_0_0
	case "V2_5_0_0":
		return &sarama.V2_5_0_0
	}
	return nil
}
