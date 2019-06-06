package kafka

import (
	"github.com/obase/conf"
	"strconv"
	"sync"
)

const PCKEY = "kafkaProducer"
const CCKEY = "kafkaConsumer"

var once sync.Once

func Init() {
	once.Do(func() {
		conf.Init()
		configs, ok := conf.GetSlice(PCKEY)
		if ok && len(configs) > 0 {
			for i, c := range configs {
				key, ok := conf.ElemString(c, "key")
				if !ok {
					panic("Undef kafka producder key of " + strconv.Itoa(i))
				}
				address, ok := conf.ElemStringSlice(c, "address")
				async, ok := conf.ElemBool(c, "async")
				asyncReturnSuccess, ok := conf.ElemBool(c, "asyncReturnSuccess")
				asyncReturnError, ok := conf.ElemBool(c, "asyncReturnError")

				err := SetupProducer(&ProducerOption{
					Key:                key,
					Address:            address,
					Async:              async,
					AsyncReturnSuccess: asyncReturnSuccess,
					AsyncReturnError:   asyncReturnError,
				})
				if err != nil {
					panic("Setup kafka producer error: " + key)
				}
			}
		}
		configs, ok = conf.GetSlice(CCKEY)
		if ok && len(configs) > 0 {
			for i, c := range configs {
				key, ok := conf.ElemString(c, "key")
				if !ok {
					panic("Undef kafka consumer key " + strconv.Itoa(i))
				}
				address, ok := conf.ElemStringSlice(c, "address")
				group, ok := conf.ElemString(c, "group")
				offset, ok := conf.ElemInt(c, "offset")

				err := SetupConsumer(&ConsumerOption{
					Key:     key,
					Address: address,
					Group:   group,
					Offset:  int64(offset),
				})
				if err != nil {
					panic("Setup kafka consumer error: " + key)
				}
			}
		}
	})
}
