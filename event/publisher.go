package event

import (
	"github.com/Rock-liyi/p2pdb/infrastructure/util/function"
	api "github.com/Rock-liyi/p2pdb/interface/api"
)

// type DMLType struct {
// 	INSERT string
// 	UPDATE string
// 	DELETE string
// }

// type DDLType struct {
// 	CREATE        string
// 	ALTER         string
// 	DROP          string
// 	DDLActionType DDLActionType
// }

// type DDLActionType struct {
// 	DATABASE string
// 	TABLE    string
// }

func PublishSyncEvent(eventType string, data interface{}) {
	var eventApi = api.EventApi{}
	var newData = function.JsonEncode(data)
	eventApi.PublishSyncEvent(eventType, newData)
}
