package event

import (
	"github.com/Rock-liyi/p2pdb/application/event"
)

func PublishSyncEvent(eventType string, data interface{}) {
	event.PublishSyncEvent(eventType, event.Message{eventType, data})
}
