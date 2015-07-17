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
	var condition backend.DeleteCondition
	params := op.params

	switch {
	case params.PrevValue != nil:
		condition = backend.PrevValue(*params.PrevValue)
	case params.PrevIndex != nil:
		condition = backend.PrevIndex(*params.PrevIndex)
	default:
		condition = backend.Always
	}

	var node *models.Node
	var index int64
	var err error

	if params.Dir || params.Recursive {
		node, index, err = op.Store.RmDir(params.Key, params.Recursive, condition)
	} else {
		node, index, err = op.Store.Delete(params.Key, condition)
	}
	if err != nil {
		return nil, err
	}

	return &models.ActionUpdate{
		Action: condition.DeleteActionName(),
		Node: models.Node{
			Key:           params.Key,
			CreatedIndex:  node.CreatedIndex,
			ModifiedIndex: index,
		},
		PrevNode: node,
	}, nil
}
