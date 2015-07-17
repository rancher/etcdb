package operations

import (
	"github.com/rancher/etcdb/backend"
	"github.com/rancher/etcdb/models"
)

type DeleteNode struct {
	params struct {
		Key       string  `path:"key"`
		PrevValue *string `query:"prevValue"`
		PrevIndex *int64  `query:"prevIndex"`
		Dir       bool    `query:"dir"`
		Recursive bool    `query:"recursive"`
	}
	Store *backend.SqlBackend
}

func (op *DeleteNode) Params() interface{} {
	return &op.params
}

func (op *DeleteNode) Call() (interface{}, error) {
	var condition backend.Condition
	params := op.params

	switch {
	case params.PrevValue != nil:
		condition = backend.PrevValue(*params.PrevValue)
	case params.PrevIndex != nil:
		condition = backend.PrevIndex(*params.PrevIndex)
	default:
		condition = backend.Always
	}

	node, index, err := op.Store.Delete(params.Key, condition)
	if err != nil {
		return nil, err
	}

	return &models.ActionUpdate{
		Action: "delete",
		Node: models.Node{
			Key:           params.Key,
			CreatedIndex:  node.CreatedIndex,
			ModifiedIndex: index,
		},
		PrevNode: node,
	}, nil
}
