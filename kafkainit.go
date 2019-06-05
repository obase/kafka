package kafka

import (
	"github.com/obase/conf"
	"sync"
)

const PCKEY = "kafkaProducer"
const CCKEY = "kafkaConsumer"

var once sync.Once

func Init() {
	once.Do(func() {
		conf.Init()
		configs, ok := conf.GetSlice(CKEY)
		if !ok || len(configs) == 0 {
			return
		}
		//for _, config := range configs {
		//	if key, ok := conf.ElemString(config, "key"); ok {
		//
		//		address, ok := conf.ElemStringSlice(config, "address")
		//		groupId, ok := conf.ElemString("groupId", "") // 可选
		//
		//
		//		//////////////////////////////
		//
		//		database, ok := conf.ElemString(config, "database")
		//		username, ok := conf.ElemString(config, "username")
		//		password, ok := conf.ElemString(config, "password")
		//		source, ok := conf.ElemString(config, "source")
		//		safe, ok := conf.ElemMap(config, "safe")
		//		mode, ok := conf.Elem(config, "mode")
		//
		//	}
		//}
	})
}
