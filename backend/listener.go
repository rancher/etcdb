package backend

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/rancher/etcdb/models"
)

// A ChangeWatcher monitors the store's changes table to serve watch results
type ChangeWatcher struct {
	store         *SqlBackend
	changes       *changeList
	watch         chan *watch
	watches       map[*watch]struct{}
	refreshPeriod time.Duration
	lastIndex     int64
	stop          chan struct{}
}

// Watch creates and starts a new ChangeWatcher for the SqlBackend
func Watch(store *SqlBackend, refreshPeriod time.Duration) *ChangeWatcher {
	cw := &ChangeWatcher{
		store:         store,
		watch:         make(chan *watch),
		refreshPeriod: refreshPeriod,
		stop:          make(chan struct{}),
		watches:       make(map[*watch]struct{}),
		changes:       newChangeList(MaxChanges),
	}
	go cw.Run()
	return cw
}

// Stop stops the ChangeWatcher's Run loop
func (cw *ChangeWatcher) Stop() {
	close(cw.stop)
}

// NextChange waits for a matching change event, and returns an ActionUpdate
// with the change data
func (cw *ChangeWatcher) NextChange(key string, recursive bool, index int64) (*models.ActionUpdate, error) {
	w := NewWatch(index, key, recursive)
	cw.watch <- w
	return w.Result()
}

// Run starts the event loop to poll for changes, and receive new watch requests
func (cw *ChangeWatcher) Run() {
	cw.refresh()

	refresh := time.NewTicker(cw.refreshPeriod)

	for {
		select {
		case <-cw.stop:
			refresh.Stop()
			return
		case w := <-cw.watch:
			cw.addWatch(w)
		case <-refresh.C:
			cw.refresh()
		}
	}
}

func (cw *ChangeWatcher) addWatch(w *watch) {
	cw.watches[w] = struct{}{}

	if w.Index <= 0 || cw.changes.Size == 0 {
		return
	}

	if oldestIndex := cw.changes.First().Index; w.Index < oldestIndex {
		w.SetResult(nil, models.EventIndexCleared(oldestIndex, w.Index, cw.lastIndex))
		delete(cw.watches, w)
		return
	}

	for i := 0; i < cw.changes.Size; i++ {
		c := cw.changes.Item(i)
		if cw.checkChange(c, w) {
			break
		}
	}
}

func (cw *ChangeWatcher) checkChange(c *change, w *watch) bool {
	if !w.Match(c) {
		return false
	}

	action, err := c.Value(cw.store)
	if err == ErrChangeIndexCleared {
		// if this change was already cleared, but watch didn't specify an index,
		// just return to wait for the next matching change
		if w.Index == 0 {
			return false
		}
		err = models.EventIndexCleared(c.Index+1, w.Index, cw.lastIndex)
	}
	w.SetResult(action, err)
	delete(cw.watches, w)

	return true
}

func (cw *ChangeWatcher) refresh() {
	newCount, err := cw.fetchSince(cw.lastIndex)
	if err != nil {
		log.Println("error refreshing:", err)
		// don't return since we still want to process any changes we did get
	}
	if newCount == 0 {
		return
	}

	cw.lastIndex = cw.changes.Last().Index

	i := 0
	if newCount < cw.changes.Size {
		i = cw.changes.Size - newCount
	}

	for ; i < cw.changes.Size; i++ {
		c := cw.changes.Item(i)
		for w := range cw.watches {
			cw.checkChange(c, w)
		}
	}
}

func (cw *ChangeWatcher) fetchSince(lastIndex int64) (count int, err error) {
	// store.Begin() makes sure expired nodes are updated, even though we don't
	// really need a new transaction for this one read query
	tx, err := cw.store.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	rows, err := cw.store.Query().Extend(`
		SELECT "index", "key", "action", "prev_node_modified" FROM "changes"
		WHERE "index" > `, lastIndex, `
		ORDER BY "index"`).Query(tx)

	if err != nil {
		return 0, err
	}
	defer rows.Close()

	for rows.Next() {
		c := cw.changes.Next()
		err = rows.Scan(&c.Index, &c.Key, &c.Action, &c.PrevNodeModified)
		if err != nil {
			// remove the change w/ the error, but return count of successfully
			// added changes
			cw.changes.Pop()
			return count, err
		}
		count++
	}

	return count, nil
}

// changeList is a simple circular buffer for storing the changes.
// Old changes will automatically be overwritten when the buffer is full.
type changeList struct {
	changes  []change
	Capacity int
	Begin    int
	Size     int
}

func newChangeList(capacity int) *changeList {
	return &changeList{Capacity: capacity, changes: make([]change, capacity)}
}

func (cl *changeList) Item(i int) *change {
	return &cl.changes[(cl.Begin+i)%cl.Capacity]
}

func (cl *changeList) First() *change {
	return cl.Item(0)
}

func (cl *changeList) Last() *change {
	return cl.Item(cl.Size - 1)
}

func (cl *changeList) Pop() {
	if cl.Size == 0 {
		return
	}
	cl.Size--
}

// Next moves the last position forward by one and returns the new last item.
// If the buffer is at capacity, the first item is dropped and cleared to be
// reused.
func (cl *changeList) Next() *change {
	if cl.Size == cl.Capacity {
		cl.First().Clear()
		cl.Begin = (cl.Begin + 1) % cl.Capacity
	} else {
		cl.Size++
	}
	return cl.Last()
}

// ErrChangeIndexCleared is returned by change.Value() when one of the nodes
// referenced by the change has been cleared from the nodes table.
var ErrChangeIndexCleared = errors.New("one of the nodes for this change has been cleared")

type change struct {
	Index            int64
	Key              string
	Action           string
	PrevNodeModified *int64
	value            *models.ActionUpdate
}

// Clear resets the value pointer so that the change struct can be reused
func (c *change) Clear() {
	c.value = nil
}

// Value fetches the node values for the changes, and returns an ActionUpdate
// The result is memoized after the first call.
func (c *change) Value(store *SqlBackend) (*models.ActionUpdate, error) {
	if c.value == nil {
		isDeleteAction := false
		switch c.Action {
		case "delete", "compareAndDelete", "expire":
			isDeleteAction = true
		}

		if isDeleteAction && c.PrevNodeModified == nil {
			return nil, fmt.Errorf("action type %s should have prev_node_modified set", c.Action)
		}

		q := store.queryNodeWithDeleted().Extend(` WHERE "key" = `, c.Key, ` AND "modified" IN (`)
		if isDeleteAction {
			q.Param(c.PrevNodeModified)
		} else {
			q.Param(c.Index)
			if c.PrevNodeModified != nil {
				q.Extend(`, `, c.PrevNodeModified)
			}
		}
		q.Text(`)`)

		rows, err := q.Query(store.db)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		nodes := make(map[int64]*models.Node)

		for rows.Next() {
			node, err := scanNode(rows)
			if err != nil {
				return nil, err
			}
			nodes[node.ModifiedIndex] = node
		}

		action := models.ActionUpdate{Action: c.Action}

		if c.PrevNodeModified != nil {
			prevNode, ok := nodes[*c.PrevNodeModified]
			if !ok {
				return nil, ErrChangeIndexCleared
			}
			action.PrevNode = prevNode
		}

		if isDeleteAction {
			action.Node.Key = c.Key
			action.Node.CreatedIndex = action.PrevNode.CreatedIndex
			action.Node.ModifiedIndex = c.Index
		} else {
			node, ok := nodes[c.Index]
			if !ok {
				return nil, ErrChangeIndexCleared
			}
			action.Node = *node
		}

		c.value = &action
	}

	return c.value, nil
}

type watchResult struct {
	Action *models.ActionUpdate
	Err    error
}

type watch struct {
	Index     int64
	Key       string
	Recursive bool
	result    chan watchResult
}

func NewWatch(index int64, key string, recursive bool) *watch {
	return &watch{index, key, recursive, make(chan watchResult, 1)}
}

func (w *watch) SetResult(action *models.ActionUpdate, err error) {
	select {
	case w.result <- watchResult{action, err}:
	default:
		// drop duplicate results
	}
}

func (w *watch) Result() (*models.ActionUpdate, error) {
	res := <-w.result
	return res.Action, res.Err
}

func (w *watch) Match(c *change) bool {
	if c.Index < w.Index {
		return false
	}
	if c.Key == w.Key {
		return true
	}
	if w.Recursive && isParent(w.Key, c.Key) {
		return true
	}
	switch c.Action {
	case "delete", "expire":
		return isParent(c.Key, w.Key)
	}
	return false
}

func isParent(a, b string) bool {
	return b[:len(a)+1] == a+"/"
}
