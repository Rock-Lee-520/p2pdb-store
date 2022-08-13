package event

import (
	api "github.com/Rock-liyi/p2pdb/interface/api"
)

type Message struct {
	Type string
	Data interface{}
}

func PublishSyncEvent(eventType string, data string) {
	var eventApi = api.EventApi{}

	eventApi.PublishSyncEvent(eventType, data)
}
