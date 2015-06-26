package operations

import (
	"time"

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
	Store          *backend.SqlBackend
	WaitPollPeriod time.Duration
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

		origNode, err := op.Store.Get(op.params.Key, false)

		if modelErr, ok := err.(models.Error); ok && modelErr.ErrorCode == 100 {
			if waitIndex != 0 {
				return &models.ActionUpdate{
					Action: "delete",
					Node: models.Node{
						Key:           op.params.Key,
						ModifiedIndex: modelErr.Index,
					},
				}, nil
			}
		} else if err != nil {
			return nil, err
		} else if waitIndex == 0 {
			waitIndex = origNode.ModifiedIndex + 1
		} else if origNode.ModifiedIndex >= waitIndex {
			return &models.ActionUpdate{
				Action: "set",
				Node:   *origNode,
			}, nil
		}

		for {
			time.Sleep(op.WaitPollPeriod)

			node, err := op.Store.Get(op.params.Key, false)

			if modelErr, ok := err.(models.Error); ok && modelErr.ErrorCode == 100 {
				if waitIndex != 0 {
					return &models.ActionUpdate{
						Action: "delete",
						Node: models.Node{
							Key:           op.params.Key,
							CreatedIndex:  origNode.CreatedIndex,
							ModifiedIndex: modelErr.Index,
						},
						PrevNode: origNode,
					}, nil
				}
			} else if err != nil {
				return nil, err
			} else if node.ModifiedIndex >= waitIndex {
				return &models.ActionUpdate{
					Action:   "set",
					Node:     *node,
					PrevNode: origNode,
				}, nil
			}
		}
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
