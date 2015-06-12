package operations

import (
	"github.com/rancherio/etcdb/backend"
	"github.com/rancherio/etcdb/models"
)

type GetNode struct {
	params struct {
		Key       string `path:"key"`
		Wait      bool   `query:"wait"`
		WaitIndex *int64 `query:"waitIndex"`
		Recursive bool   `query:"recursive"`
		Sorted    bool   `query:"sorted"`
	}
	Store *backend.SqlBackend
}

func (op *GetNode) Params() interface{} {
	return &op.params
}

func (op *GetNode) Call() (interface{}, error) {
	node, err := op.Store.Get(op.params.Key, op.params.Recursive)
	if err != nil {
		return nil, err
	}

	return &models.Action{
		Action: "get",
		Node:   *node,
	}, nil
}
