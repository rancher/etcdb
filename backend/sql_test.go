package backend

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/rancherio/etcdb/models"
)

func TestMain(m *testing.M) {
	dbDriver = "postgres"
	dbDataSource = "sslmode=disable database=etcd_test"
	log.Println("Running PostgreSQL tests")
	postgresResult := m.Run()

	dbDriver = "mysql"
	dbDataSource = "root@/etcd_test"
	log.Println("Running MySQL tests")
	mysqlResult := m.Run()

	os.Exit(mysqlResult | postgresResult)
}

var dbDriver, dbDataSource string

func testConn(t *testing.T) *SqlBackend {
	store, err := New(dbDriver, dbDataSource)
	ok(t, err)
	err = store.dropSchema()
	ok(t, err)
	err = store.CreateSchema()
	ok(t, err)

	return store
}

func TestGetMissingReturnsNotFound(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, err := store.Get("/foo", false)
	expectError(t, "Key not found", "/foo", err)
}

func TestSet(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	node, prevNode, err := store.Set("/foo", "bar", Always)
	ok(t, err)

	if node == nil {
		fatalf(t, "node should not be nil")
	}

	if prevNode != nil {
		fatalf(t, "setting new node should return nil for prevNode, but got: %#v", prevNode)
	}

	equals(t, "/foo", node.Key)
	equals(t, "bar", node.Value)
}

func TestSetThenGet(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, prevNode, err := store.Set("/foo", "bar", Always)
	ok(t, err)

	if prevNode != nil {
		fatalf(t, "setting new node should return nil for prevNode, but got: %#v", prevNode)
	}

	node, err := store.Get("/foo", false)
	ok(t, err)

	equals(t, "/foo", node.Key)
	equals(t, "bar", node.Value)
}

func TestFullCycle(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	node, err := store.Get("/foo", false)

	if node != nil {
		fatalf(t, "node should be nil before set, but got: %#v", node)
	}

	_, prevNode, err := store.Set("/foo", "bar", Always)
	ok(t, err)

	if prevNode != nil {
		fatalf(t, "setting new node should return nil for prevNode, but got: %#v", prevNode)
	}

	node, err = store.Get("/foo", false)

	equals(t, "/foo", node.Key)
	equals(t, "bar", node.Value)

	prevNode, _, err = store.Delete("/foo", Always)
	ok(t, err)

	equals(t, "/foo", prevNode.Key)
	equals(t, "bar", prevNode.Value)

	node, err = store.Get("/foo", false)

	if node != nil {
		fatalf(t, "node should be nil after deleting")
	}
}

func TestSet_PrevExist_True_Success(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo", "original", Always)
	ok(t, err)

	node, prevNode, err := store.Set("/foo", "updated", PrevExist(true))
	ok(t, err)

	equals(t, "/foo", node.Key)
	equals(t, "updated", node.Value)

	equals(t, "/foo", prevNode.Key)
	equals(t, "original", prevNode.Value)
}

func TestSet_PrevExist_True_Fail(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo", "updated", PrevExist(true))
	expectError(t, "Key not found", "/foo", err)
}

func TestSet_PrevExist_False_Success(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	node, prevNode, err := store.Set("/foo", "bar", PrevExist(false))
	ok(t, err)

	equals(t, "/foo", node.Key)
	equals(t, "bar", node.Value)

	if prevNode != nil {
		fatalf(t, "expected prevNode to be nil, but got: %#v", prevNode)
	}
}

func TestSet_PrevExist_False_Fail(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo", "original", Always)
	ok(t, err)

	_, _, err = store.Set("/foo", "updated", PrevExist(false))
	expectError(t, "Key already exists", "/foo", err)
}

func TestSet_PrevValue_Success(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo", "original", Always)
	ok(t, err)

	node, prevNode, err := store.Set("/foo", "updated", PrevValue("original"))
	ok(t, err)

	equals(t, "/foo", node.Key)
	equals(t, "updated", node.Value)

	equals(t, "/foo", prevNode.Key)
	equals(t, "original", prevNode.Value)
}

func TestSet_PrevValue_Fail_Missing(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo", "updated", PrevValue("does not exist"))
	expectError(t, "Key not found", "/foo", err)
}

func TestSet_PrevValue_Fail_ValueMismatch(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo", "original", Always)
	ok(t, err)

	_, _, err = store.Set("/foo", "updated", PrevValue("different value"))
	expectError(t, "Compare failed", "[different value != original]", err)
}

func TestSet_PrevIndex_Success(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	node, _, err := store.Set("/foo", "original", Always)
	ok(t, err)

	node, prevNode, err := store.Set("/foo", "updated", PrevIndex(node.ModifiedIndex))
	ok(t, err)

	equals(t, "/foo", node.Key)
	equals(t, "updated", node.Value)

	equals(t, "/foo", prevNode.Key)
	equals(t, "original", prevNode.Value)
}

func TestSet_PrevIndex_Fail_Missing(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo", "updated", PrevIndex(0))
	expectError(t, "Key not found", "/foo", err)
}

func TestSet_PrevIndex_Fail_IndexMismatch(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo", "original", Always)
	ok(t, err)

	_, _, err = store.Set("/foo", "updated", PrevIndex(100))
	expectError(t, "Compare failed", "[100 != 1]", err)
}

func TestDelete_PrevValue_Success(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo", "original", Always)
	ok(t, err)

	prevNode, _, err := store.Delete("/foo", PrevValue("original"))
	ok(t, err)

	equals(t, "/foo", prevNode.Key)
	equals(t, "original", prevNode.Value)
}

func TestDelete_PrevValue_Fail_Missing(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Delete("/foo", PrevValue("does not exist"))
	expectError(t, "Key not found", "/foo", err)
}

func TestDelete_PrevValue_Fail_ValueMismatch(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo", "original", Always)
	ok(t, err)

	_, _, err = store.Delete("/foo", PrevValue("different value"))
	expectError(t, "Compare failed", "[different value != original]", err)
}

func TestDelete_PrevIndex_Success(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	node, _, err := store.Set("/foo", "original", Always)
	ok(t, err)

	prevNode, _, err := store.Delete("/foo", PrevIndex(node.ModifiedIndex))
	ok(t, err)

	equals(t, "/foo", prevNode.Key)
	equals(t, "original", prevNode.Value)
}

func TestDelete_PrevIndex_Fail_Missing(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Delete("/foo", PrevIndex(0))
	expectError(t, "Key not found", "/foo", err)
}

func TestDelete_PrevIndex_Fail_IndexMismatch(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo", "original", Always)
	ok(t, err)

	_, _, err = store.Delete("/foo", PrevIndex(100))
	expectError(t, "Compare failed", "[100 != 1]", err)
}

func Test_CreateDirectory_Simple(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.MkDir("/foo", Always)
	ok(t, err)

	node, err := store.Get("/foo", false)
	ok(t, err)

	equals(t, true, node.Dir)
	equals(t, 0, len(node.Nodes))
}

func Test_CreateDirectory_ReplacesFile(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo", "original", Always)
	ok(t, err)

	node, prevNode, err := store.MkDir("/foo", Always)
	ok(t, err)

	equals(t, true, node.Dir)
	equals(t, false, prevNode.Dir)
	equals(t, "original", prevNode.Value)
}

func Test_CreateDirectory_DoesNotReplaceDir(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.MkDir("/foo", Always)
	ok(t, err)

	_, _, err = store.MkDir("/foo", Always)
	expectError(t, "Not a file", "/foo", err)
}

func Test_CreateDirectory_IfNotExist(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.MkDir("/foo", Always)
	ok(t, err)

	_, _, err = store.MkDir("/foo", PrevExist(false))
	expectError(t, "Key already exists", "/foo", err)
}

func Test_Get_ListDirectory(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.MkDir("/foo", Always)
	ok(t, err)

	_, _, err = store.Set("/foo/bar", "value", Always)
	ok(t, err)

	node, err := store.Get("/foo", false)
	ok(t, err)

	equals(t, true, node.Dir)
	equals(t, 1, len(node.Nodes))

	equals(t, "/foo/bar", node.Nodes[0].Key)
	equals(t, "value", node.Nodes[0].Value)
}

func Test_Get_ListDirectory_NotRecursive(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.MkDir("/foo", Always)
	ok(t, err)

	_, _, err = store.MkDir("/foo/bar", Always)
	ok(t, err)

	_, _, err = store.Set("/foo/bar/baz", "value", Always)
	ok(t, err)

	node, err := store.Get("/foo", false)
	ok(t, err)

	equals(t, true, node.Dir)
	equals(t, 1, len(node.Nodes))

	child := node.Nodes[0]

	equals(t, "/foo/bar", child.Key)
	equals(t, true, child.Dir)
	equals(t, 0, len(child.Nodes))
}

func Test_Get_ListDirectory_Recursive(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.MkDir("/foo", Always)
	ok(t, err)

	_, _, err = store.MkDir("/foo/bar", Always)
	ok(t, err)

	_, _, err = store.Set("/foo/bar/baz", "value", Always)
	ok(t, err)

	node, err := store.Get("/foo", true)
	ok(t, err)

	equals(t, true, node.Dir)
	equals(t, 1, len(node.Nodes))

	child := node.Nodes[0]

	equals(t, "/foo/bar", child.Key)
	equals(t, true, child.Dir)
	equals(t, 1, len(child.Nodes))

	grandchild := child.Nodes[0]

	equals(t, "/foo/bar/baz", grandchild.Key)
	equals(t, false, grandchild.Dir)
	equals(t, "value", grandchild.Value)
	equals(t, 0, len(grandchild.Nodes))
}

func Test_Set_CreatesParentDirectories(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo/bar/baz", "value", Always)
	ok(t, err)

	node, err := store.Get("/foo", true)
	ok(t, err)

	equals(t, true, node.Dir)
	equals(t, 1, len(node.Nodes))

	child := node.Nodes[0]

	equals(t, "/foo/bar", child.Key)
	equals(t, true, child.Dir)
	equals(t, 1, len(child.Nodes))

	grandchild := child.Nodes[0]

	equals(t, "/foo/bar/baz", grandchild.Key)
	equals(t, false, grandchild.Dir)
	equals(t, "value", grandchild.Value)
	equals(t, 0, len(grandchild.Nodes))

	equals(t, grandchild.CreatedIndex, node.CreatedIndex)
	equals(t, grandchild.ModifiedIndex, node.ModifiedIndex)
}

func Test_Set_CreatesParentDirectories_GetNonRecursive(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo/bar/baz", "value", Always)
	ok(t, err)

	node, err := store.Get("/foo", false)
	ok(t, err)

	if node == nil {
		fatalf(t, "expected a directory, but got nil")
	}

	equals(t, true, node.Dir)
	equals(t, 1, len(node.Nodes))

	child := node.Nodes[0]

	equals(t, "/foo/bar", child.Key)
	equals(t, true, child.Dir)
	equals(t, 0, len(child.Nodes))
}

func Test_Set_DoesNotOverwriteParentFile(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo", "value", Always)
	ok(t, err)

	_, _, err = store.Set("/foo/bar", "value", Always)
	expectError(t, "Not a directory", "/foo", err)
}

func Test_MkDir_DoesNotOverwriteParentFile(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo", "value", Always)
	ok(t, err)

	_, _, err = store.MkDir("/foo/bar", Always)
	expectError(t, "Not a directory", "/foo", err)
}

func Test_Delete_DoesNotRemoveDirectory(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.MkDir("/foo", Always)
	ok(t, err)

	_, _, err = store.Delete("/foo", Always)
	expectError(t, "Not a file", "/foo", err)
}

// XXX this is kind of weird, but dir=true can also delete files
func Test_RmDir_CanRemoveFile(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo", "value", Always)
	ok(t, err)

	_, _, err = store.RmDir("/foo", false, Always)
	ok(t, err)

	_, err = store.Get("/foo", false)
	expectError(t, "Key not found", "/foo", err)
}

func Test_RmDir_CanRemoveEmptyDirectory(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.MkDir("/foo", Always)
	ok(t, err)

	_, _, err = store.RmDir("/foo", false, Always)
	ok(t, err)
}

func Test_RmDir_DoesNotRemoveNonEmptyDirectory(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo/bar", "value", Always)
	ok(t, err)

	_, _, err = store.RmDir("/foo", false, Always)
	expectError(t, "Directory not empty", "/foo", err)
}

func Test_RmDir_Recursive(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.Set("/foo/bar", "value", Always)
	ok(t, err)

	_, _, err = store.RmDir("/foo", true, Always)
	ok(t, err)

	_, err = store.Get("/foo", false)
	expectError(t, "Key not found", "/foo", err)

	_, err = store.Get("/foo/bar", false)
	expectError(t, "Key not found", "/foo/bar", err)
}

func Test_TTL_SetsExpiration(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.SetTTL("/foo", "value", 100, Always)
	ok(t, err)

	node, err := store.Get("/foo", false)
	ok(t, err)

	equals(t, int64(100), *node.TTL)
	if node.Expiration.IsZero() {
		fatalf(t, "expected Expiration to have a non-zero value")
	}

	diff := node.Expiration.Sub(time.Now().UTC())
	if diff.Seconds() > 110 || diff.Seconds() < 90 {
		fatalf(t, "expected Expiration to occur in ~100s, but got: %d", diff)
	}
}

func Test_TTL_SetThenClear(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.SetTTL("/foo", "value", 100, Always)
	ok(t, err)

	node, err := store.Get("/foo", false)
	ok(t, err)

	equals(t, int64(100), *node.TTL)
	if node.Expiration.IsZero() {
		fatalf(t, "expected Expiration to have a non-zero value")
	}

	// Should clear the TTL
	_, _, err = store.Set("/foo", "value", Always)

	node, err = store.Get("/foo", false)
	ok(t, err)

	if node.TTL != nil {
		fatalf(t, "expected TTL to be nil, but got: %d", *node.TTL)
	}
	if node.Expiration != nil {
		fatalf(t, "expected Expiration to be nil, but got: %s", node.Expiration)
	}
}

func Test_TTL_CountsDown(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.SetTTL("/foo", "value", 100, Always)
	ok(t, err)

	node, err := store.Get("/foo", false)
	ok(t, err)
	equals(t, int64(100), *node.TTL)

	// MySQL only stores to 1-second precision, so sleep long enough
	// to make sure there's no chance of truncation error
	time.Sleep(2 * time.Second)

	node, err = store.Get("/foo", false)
	ok(t, err)

	if !(*node.TTL < 100) {
		fatalf(t, "expected TTL to have decreased, but got: %d", *node.TTL)
	}
}

func Test_TTL_NodeExpires(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	_, _, err := store.SetTTL("/foo", "value", 1, Always)
	ok(t, err)

	node, err := store.Get("/foo", false)
	ok(t, err)
	equals(t, int64(1), *node.TTL)

	// MySQL only stores to 1-second precision, so sleep long enough
	// to make sure there's no chance of truncation error
	time.Sleep(2 * time.Second)

	_, err = store.Get("/foo", false)
	expectError(t, "Key not found", "/foo", err)
}

func Test_CreateInOrder(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	node1, err := store.CreateInOrder("/foo", "value", nil)
	ok(t, err)

	equals(t, int64(1), node1.CreatedIndex)
	equals(t, "/foo/1", node1.Key)
	equals(t, "value", node1.Value)

	node2, err := store.CreateInOrder("/foo", "value", nil)
	ok(t, err)

	equals(t, int64(2), node2.CreatedIndex)
	equals(t, "/foo/2", node2.Key)
	equals(t, "value", node2.Value)
}

func Test_CreateInOrder_TTL(t *testing.T) {
	store := testConn(t)
	defer store.Close()

	ttl := int64(100)
	node, err := store.CreateInOrder("/foo", "value", &ttl)
	ok(t, err)

	equals(t, "/foo/1", node.Key)
	equals(t, ttl, *node.TTL)
	if node.Expiration.IsZero() {
		fatalf(t, "expected Expiration to have a non-zero value")
	}
}

func fatalf(tb testing.TB, format string, args ...interface{}) {
	fatalfLvl(1, tb, format, args...)
}

func fatalfLvl(lvl int, tb testing.TB, format string, args ...interface{}) {
	_, file, line, _ := runtime.Caller(lvl + 1)
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("\033[31m%s:%d:%s\033[39m\n\n", filepath.Base(file), line, msg)
	tb.FailNow()
}

func expectError(tb testing.TB, message, cause string, err error) {
	if modelError, ok := err.(models.Error); ok {
		if modelError.Message != message {
			fatalfLvl(1, tb, "\n\n\texpected Message: %#v\n\n\tgot: %#v", message, modelError.Message)
		}
		if modelError.Cause != cause {
			fatalfLvl(1, tb, "\n\n\texpected Cause: %#v\n\n\tgot: %#v", cause, modelError.Cause)
		}
	} else {
		fatalfLvl(1, tb, "expected models.Error, but got %T %#v", err, err)
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		fatalfLvl(1, tb, " unexpected error: %s", err.Error())
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		fatalfLvl(1, tb, "\n\n\texp: %#v\n\n\tgot: %#v", exp, act)
	}
}
