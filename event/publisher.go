package event

import (
	"github.com/Rock-liyi/p2pdb/application/event"
)

func PublishSyncEvent(eventType string, data interface{}) {
	msg := event.Message{Type: eventType, Data: data}
	event.PublishSyncEvent(eventType, msg)
}
