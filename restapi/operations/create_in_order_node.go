package operations

import (
	"github.com/rancherio/etcdb/backend"
	"github.com/rancherio/etcdb/models"
)

type CreateInOrderNode struct {
	params struct {
		Key   string `path:"key"`
		Value string `formData:"value"`
		TTL   *int64 `formData:"ttl"`
	}
	Store *backend.SqlBackend
}

func (op *CreateInOrderNode) Params() interface{} {
	return &op.params
}

func (op *CreateInOrderNode) Call() (interface{}, error) {
	node, err := op.Store.CreateInOrder(op.params.Key, op.params.Value, op.params.TTL)
	if err != nil {
		return nil, err
	}

	return &models.Action{
		Action: "create",
		Node:   *node,
	}, nil
}
