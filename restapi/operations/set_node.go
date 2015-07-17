package operations

import (
	"github.com/rancher/etcdb/backend"
	"github.com/rancher/etcdb/models"
)

type SetNode struct {
	params struct {
		Key       string  `path:"key"`
		Value     string  `formData:"value"`
		TTL       *int64  `formData:"ttl"`
		Dir       bool    `formData:"dir"`
		PrevValue *string `formData:"prevValue"`
		PrevIndex *int64  `formData:"prevIndex"`
		PrevExist *bool   `formData:"prevExist"`
	}
	Store *backend.SqlBackend
}

func (op *SetNode) Params() interface{} {
	return &op.params
}

func (op *SetNode) Call() (interface{}, error) {
	var condition backend.Condition
	params := op.params

	switch {
	case params.PrevExist != nil:
		condition = backend.PrevExist(*params.PrevExist)
	case params.PrevValue != nil:
		condition = backend.PrevValue(*params.PrevValue)
	case params.PrevIndex != nil:
		condition = backend.PrevIndex(*params.PrevIndex)
	default:
		condition = backend.Always
	}

	var node, prevNode *models.Node
	var err error

	if params.Dir {
		node, prevNode, err = op.Store.MkDir(params.Key, params.TTL, condition)
	} else if params.TTL != nil {
		node, prevNode, err = op.Store.SetTTL(params.Key, params.Value, *params.TTL, condition)
	} else {
		node, prevNode, err = op.Store.Set(params.Key, params.Value, condition)
	}

	if err != nil {
		return nil, err
	}

	return &models.ActionUpdate{
		Action:   "set",
		Node:     *node,
		PrevNode: prevNode,
	}, nil
}
