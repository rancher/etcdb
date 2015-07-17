package backend

import (
	"testing"
	"time"

	"github.com/rancher/etcdb/models"
)

func Test_Watch_Change(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	cw := Watch(store, 1*time.Second)
	defer cw.Stop()

	go func() {
		time.Sleep(10 * time.Millisecond)
		store.Set("/foo", "bar", Always)
	}()

	act, err := cw.NextChange("/foo", false, int64(0))
	ok(t, err)

	equals(t, "/foo", act.Node.Key)
	equals(t, "bar", act.Node.Value)
}

func Test_Watch_ReturnsFirstMatchingChange(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	cw := Watch(store, 1*time.Second)
	defer cw.Stop()

	store.Set("/foo", "first", Always)
	store.Set("/foo", "second", Always)
	time.Sleep(2 * time.Second)

	act, err := cw.NextChange("/foo", false, int64(1))
	ok(t, err)

	equals(t, "/foo", act.Node.Key)
	equals(t, "first", act.Node.Value)
}

func Test_Watch_IgnoresOldChangeWhenIndexNotSet(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	cw := Watch(store, 1*time.Second)
	defer cw.Stop()

	store.Set("/foo", "first", Always)
	time.Sleep(2 * time.Second)

	go func() {
		time.Sleep(10 * time.Millisecond)
		store.Set("/foo", "second", Always)
	}()

	act, err := cw.NextChange("/foo", false, int64(0))
	ok(t, err)

	equals(t, "/foo", act.Node.Key)
	equals(t, "second", act.Node.Value)
}

func Test_ChangeList_Empty(t *testing.T) {
	cl := newChangeList(100)
	equals(t, 0, cl.Size)
}

func Test_ChangeList_AddOne(t *testing.T) {
	cl := newChangeList(100)
	change := cl.Next()

	equals(t, 1, cl.Size)
	equals(t, int64(0), change.Index)

	if cl.First() != change {
		t.Error("First should be the same change")
	}

	if cl.Last() != change {
		t.Error("Last should be the same change")
	}
}

func Test_ChangeList_FirstLast(t *testing.T) {
	cl := newChangeList(100)
	first := cl.Next()
	second := cl.Next()

	if cl.First() != first {
		t.Error("First should be the same change")
	}

	if cl.Last() != second {
		t.Error("Last should be the same change")
	}

	if first == second {
		t.Error("first and second should be different changes")
	}
}

func Test_ChangeList_WrapAround(t *testing.T) {
	cl := newChangeList(2)
	first := cl.Next()
	second := cl.Next()
	third := cl.Next()

	if first != third {
		t.Error("first and third should the same change")
	}

	if cl.First() != second {
		t.Error("the First() position should have been incremented to where second was")
	}
}

func Test_ChangeList_FirstWrapAround(t *testing.T) {
	cl := newChangeList(2)
	cl.Next()
	cl.Next()

	equals(t, cl.Size, cl.Capacity)

	first := cl.First()

	if first != cl.Next() {
		t.Error("the first item before should now be the next item")
	}
}

func Test_ChangeList_WrapAroundClearsValue(t *testing.T) {
	cl := newChangeList(2)
	first := cl.Next()
	first.value = &models.ActionUpdate{}

	_ = cl.Next()
	third := cl.Next()

	if first != third {
		t.Error("first and third should the same change")
	}
	if first.value != nil {
		t.Error("value should be reset to nil")
	}
	if third.value != nil {
		t.Error("value should be reset to nil")
	}
}

func Test_ChangeList_Pop(t *testing.T) {
	cl := newChangeList(100)
	first := cl.Next()
	_ = cl.Next()

	equals(t, 2, cl.Size)

	cl.Pop()

	equals(t, 1, cl.Size)

	if cl.Last() != first {
		t.Error("after pop, last element should be first again")
	}
}

func Test_ChangeList_PopEmpty(t *testing.T) {
	cl := newChangeList(100)
	cl.Pop()
	equals(t, 0, cl.Size)
}

func Test_Match_Same(t *testing.T) {
	w := &watch{Key: "/foo", Index: 1}
	c := &change{Key: "/foo", Index: 1, Action: "set"}
	equals(t, true, w.Match(c))
}

func Test_Match_SubkeyNotRecursive(t *testing.T) {
	w := &watch{Key: "/foo"}
	c := &change{Key: "/foo/bar", Index: 1, Action: "set"}
	equals(t, false, w.Match(c))
}

func Test_Match_SubkeyRecursive(t *testing.T) {
	w := &watch{Key: "/foo", Index: 1, Recursive: true}
	c := &change{Key: "/foo/bar", Index: 1, Action: "set"}
	equals(t, true, w.Match(c))
}

func Test_Match_PrefixRecursive(t *testing.T) {
	w := &watch{Key: "/foo", Recursive: true}
	c := &change{Key: "/foobar", Index: 1, Action: "set"}
	equals(t, false, w.Match(c))
}

func Test_Match_SameKeyRecursive(t *testing.T) {
	w := &watch{Key: "/foo", Recursive: true}
	c := &change{Key: "/foo", Index: 1, Action: "set"}
	equals(t, true, w.Match(c))
}

func Test_Match_LowerIndex(t *testing.T) {
	w := &watch{Key: "/foo", Index: 1}
	c := &change{Key: "/foo", Index: 2, Action: "set"}
	equals(t, true, w.Match(c))
}

func Test_Match_HigherIndex(t *testing.T) {
	w := &watch{Key: "/foo", Index: 2}
	c := &change{Key: "/foo", Index: 1, Action: "set"}
	equals(t, false, w.Match(c))
}

func Test_Match_SetParent(t *testing.T) {
	w := &watch{Key: "/foo/bar"}
	c := &change{Key: "/foo", Index: 1, Action: "set"}
	equals(t, false, w.Match(c))
}

func Test_Match_DeleteParent(t *testing.T) {
	w := &watch{Key: "/foo/bar"}
	c := &change{Key: "/foo", Index: 1, Action: "delete"}
	equals(t, true, w.Match(c))
}

func Test_Match_ExpireParent(t *testing.T) {
	w := &watch{Key: "/foo/bar"}
	c := &change{Key: "/foo", Index: 1, Action: "expire"}
	equals(t, true, w.Match(c))
}
