package event

import (
	"bytes"
	"math/rand"
	"testing"
	"time"
	//_ "github.com/Rock-liyi/p2pdb/application/event/subscribe" //注册事件监听
	//common_event "github.com/Rock-liyi/p2pdb/domain/common/event"
)

func randInt(min int, max int) byte {

	rand.Seed(time.Now().UnixNano())

	return byte(min + rand.Intn(max-min))

}

func randUpString(l int) []byte {

	var result bytes.Buffer

	var temp byte

	for i := 0; i < l; {

		if randInt(65, 91) != temp {

			temp = randInt(65, 91)

			result.WriteByte(temp)

			i++

		}

	}

	return result.Bytes()

}

func TestPublishAsyncEvent(t *testing.T) {
	//	data := randUpString(19)
	//PublishSyncEvent(common_event.StoreAlterTableEvent, data)
}
