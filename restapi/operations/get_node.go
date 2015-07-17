package operations

import (
	"github.com/rancher/etcdb/backend"
	"github.com/rancher/etcdb/models"
)

type GetNode struct {
	params struct {
		Key       string `path:"key"`
		Wait      bool   `query:"wait"`
		WaitIndex *int64 `query:"waitIndex"`
		Recursive bool   `query:"recursive"`
		Sorted    bool   `query:"sorted"`
	}
	Store   *backend.SqlBackend
	Watcher *backend.ChangeWatcher
}

func (op *GetNode) Params() interface{} {
	return &op.params
}

func (op *GetNode) Call() (interface{}, error) {
	if op.params.Wait {
		waitIndex := int64(0)
		if op.params.WaitIndex != nil {
			waitIndex = *op.params.WaitIndex
		}
		return op.Watcher.NextChange(op.params.Key, op.params.Recursive, waitIndex)
	}

	node, err := op.Store.Get(op.params.Key, op.params.Recursive)
	if err != nil {
		return nil, err
	}

	return &models.Action{
		Action: "get",
		Node:   *node,
	}, nil
}
