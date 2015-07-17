package backend

import "github.com/rancher/etcdb/models"

type Condition interface {
	Check(key string, index int64, node *models.Node) error
}

type SetCondition interface {
	Condition
	SetActionName() string
}

type DeleteCondition interface {
	Condition
	DeleteActionName() string
}

type always struct{}

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

type PrevValue string

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

type PrevIndex int64

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

type PrevExist bool

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
