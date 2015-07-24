package backend

import "github.com/rancher/etcdb/models"

// A Condition represents a test for whether a node operation should be applied
// based on the previous node value.
// The Check method should return nil on success, or return an error with the
// reason the check failed.
type Condition interface {
	Check(key string, index int64, node *models.Node) error
}

// A SetCondition can be used when setting a node.
// It provides the action name that should be used for the set operation.
type SetCondition interface {
	Condition
	SetActionName() string
}

// A DeleteCondition can be used when deleting a node.
// It provides the action name that should be used for the delete operation.
type DeleteCondition interface {
	Condition
	DeleteActionName() string
}

type always struct{}

// Always is a condition that always returns true.
var Always always

func (p always) Check(key string, index int64, node *models.Node) error {
	return nil
}

func (p always) SetActionName() string {
	return "set"
}

func (p always) DeleteActionName() string {
	return "set"
}

// PrevValue matches on the previous node's value.
type PrevValue string

// Check succeeds if the previous value matches the condition value.
func (p PrevValue) Check(key string, index int64, node *models.Node) error {
	if node == nil {
		return models.NotFound(key, index)
	}
	if node.Value != string(p) {
		return models.CompareFailed(string(p), node.Value, index)
	}
	return nil
}

func (p PrevValue) SetActionName() string {
	return "compareAndSwap"
}

func (p PrevValue) DeleteActionName() string {
	return "compareAndDelete"
}

// PrevIndex matches on the previous node's modifiedIndex.
type PrevIndex int64

// Check succeeds if the previous node's modifiedIndex matches.
func (p PrevIndex) Check(key string, index int64, node *models.Node) error {
	if node == nil {
		return models.NotFound(key, index)
	}
	if node.ModifiedIndex != int64(p) {
		return models.CompareFailed(int64(p), node.ModifiedIndex, index)
	}
	return nil
}

func (p PrevIndex) SetActionName() string {
	return "compareAndSwap"
}

func (p PrevIndex) DeleteActionName() string {
	return "compareAndDelete"
}

// PrevExist matches on whether there was a previous value.
type PrevExist bool

// Check succeeds if the existence of the previous node matches the condition's
// truth value.
func (p PrevExist) Check(key string, index int64, node *models.Node) error {
	if node == nil {
		if bool(p) {
			return models.NotFound(key, index)
		}
	} else if !bool(p) {
		return models.KeyExists(key, index)
	}
	return nil
}

func (p PrevExist) SetActionName() string {
	if bool(p) {
		return "update"
	}
	return "create"
}
