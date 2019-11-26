package kafka

import (
	"fmt"
	"github.com/obase/conf"
	"strconv"
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

			err := SetupProducer(&ProducerConfig{
				Key:           key,
				Address:       address,
				Async:         async,
				ReturnSuccess: returnSuccess,
				ReturnError:   returnError,
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
			})
			if err != nil {
				Close()
				panic(fmt.Sprintf("Setup kafka consumer failed, key=%v, err=%v ", key, err))
			}
		}
	}
}
