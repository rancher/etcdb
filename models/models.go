package models

import (
	"fmt"
	"time"
)

type Action struct {
	Action string `json:"action"`
	Node   Node   `json:"node"`
}

type ActionUpdate struct {
	Action   string `json:"action"`
	Node     Node   `json:"node"`
	PrevNode *Node  `json:"prevNode,omitempty"`
}

type Node struct {
	Key           string     `json:"key"`
	Value         string     `json:"value"`
	CreatedIndex  int64      `json:"createdIndex"`
	ModifiedIndex int64      `json:"modifiedIndex"`
	Dir           bool       `json:"dir"`
	TTL           *int64     `json:"ttl,omitempty"`
	Expiration    *time.Time `json:"expiration,omitempty"`
	Nodes         []*Node    `json:"nodes,omitempty"`
}

// TODO could reuse implementations from etcd code itself?

type Error struct {
	ErrorCode int    `json:"errorCode"`
	Message   string `json:"message"`
	Cause     string `json:"cause,omitempty"`
	// FIXME should be uint64
	Index int64 `json:"index"`
}

func (e Error) Error() string {
	return fmt.Sprintf("etcd error (%d) at index %d %s: %s", e.ErrorCode, e.Index, e.Message, e.Cause)
}

func NotFound(key string, index int64) Error {
	return Error{100, "Key not found", key, index}
}

func CompareFailed(expected, actual interface{}, index int64) Error {
	return Error{101, "Compare failed", fmt.Sprintf("[%v != %v]", expected, actual), index}
}

func NotAFile(key string, index int64) Error {
	return Error{102, "Not a file", key, index}
}

func NotADirectory(key string, index int64) Error {
	return Error{104, "Not a directory", key, index}
}

func KeyExists(key string, index int64) Error {
	return Error{105, "Key already exists", key, index}
}

func DirectoryNotEmpty(key string, index int64) Error {
	return Error{108, "Directory not empty", key, index}
}

func InvalidField(cause string) Error {
	return Error{209, "Invalid field", cause, 0}
}

func RaftInternalError(cause string) Error {
	return Error{300, "Raft Internal Error", cause, 0}
}
