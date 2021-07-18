package rbtree

import (
	"fmt"
	"github.com/scottcagno/leviathan/pkg/util"
	"log"
	"testing"
)

const (
	thousand = 1000
	n        = 1
)

func TestNewRBTree(t *testing.T) {
	var tree *RBTree
	util.AssertNil(t, tree)
	tree = NewRBTree()
	util.AssertNotNil(t, tree)
	tree.Close()
}

// signature: Has(key string) (bool, int64)
func TestRbTree_Has(t *testing.T) {
	tree := NewRBTree()
	util.AssertLen(t, 0, tree.Len())
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i), makeVal(i))
	}
	for i := 0; i < n*thousand; i++ {
		ok, _ := tree.Has(makeKey(i))
		if !ok { // existing=updated
			t.Errorf("has: %v", ok)
		}
	}
	util.AssertLen(t, n*thousand, tree.Len())
	tree.Close()
}

// signature: Put(key string, val []byte) ([]byte, bool)
func TestRbTree_Put(t *testing.T) {
	tree := NewRBTree()
	util.AssertLen(t, 0, tree.Len())
	for i := 0; i < n*thousand; i++ {
		_, existing := tree.Put(makeKey(i), makeVal(i))
		if existing { // existing=updated
			t.Errorf("putting: %v", existing)
		}
	}
	util.AssertLen(t, n*thousand, tree.Len())
	tree.Close()
}

// signature: Get(key string) ([]byte, bool)
func TestRbTree_Get(t *testing.T) {
	tree := NewRBTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	for i := 0; i < n*thousand; i++ {
		val, ok := tree.Get(makeKey(i))
		if !ok {
			t.Errorf("getting: %v", ok)
		}
		util.AssertEqual(t, makeVal(i), val)
	}
	tree.Close()
}

// signature: Del(key string) ([]byte, bool)
func TestRbTree_Del(t *testing.T) {
	tree := NewRBTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	for i := 0; i < n*thousand; i++ {
		_, ok := tree.Del(makeKey(i))
		if !ok {
			t.Errorf("delete: %v", ok)
		}
	}
	util.AssertLen(t, 0, tree.Len())
	tree.Close()
}

// signature: Len() int
func TestRbTree_Len(t *testing.T) {
	tree := NewRBTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	tree.Close()
}

// signature: Size() int64
func TestRbTree_Size(t *testing.T) {
	tree := NewRBTree()
	var numBytes int64
	for i := 0; i < n*thousand; i++ {
		key, val := makeKey(i), makeVal(i)
		numBytes += int64(len(key) + len(val))
		tree.Put(key, val)
	}
	util.AssertLen(t, numBytes, tree.Size())
	tree.Close()
}

// signature: Min() (Entry, bool)
func TestRbTree_Min(t *testing.T) {
	tree := NewRBTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	e, ok := tree.Min()
	if !ok {
		t.Errorf("min: %v", tree)
	}
	util.AssertEqual(t, makeKey(0), e.Key)
	tree.Close()
}

// signature: Max() (Entry, bool)
func TestRbTree_Max(t *testing.T) {
	tree := NewRBTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	e, ok := tree.Max()
	if !ok {
		t.Errorf("min: %v", tree)
	}
	util.AssertEqual(t, makeKey(n*thousand-1), e.Key)
	tree.Close()
}

// signature: ScanFront(iter Iterator)
func TestRbTree_ScanFront(t *testing.T) {
	tree := NewRBTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())

	printInfo := true

	// do scan front
	tree.ScanFront(func(e Entry) bool {
		if e.Key == "" {
			t.Errorf("scan front, issue with key: %v", e.Key)
			return false
		}
		if printInfo {
			log.Printf("key: %s\n", e.Key)
		}
		return true
	})

	tree.Close()
}

// signature: ScanBack(iter Iterator)
func TestRbTree_ScanBack(t *testing.T) {
	tree := NewRBTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())

	printInfo := true

	tree.ScanBack(func(e Entry) bool {
		if e.Key == "" {
			t.Errorf("scan back, issue with key: %v", e.Key)
			return false
		}
		if printInfo {
			log.Printf("key: %s\n", e.Key)
		}
		return true
	})

	tree.Close()
}

// signature: ScanRange(start Entry, end Entry, iter Iterator)
func TestRbTree_ScanRange(t *testing.T) {
	tree := NewRBTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())

	printInfo := true

	start := Entry{Key: makeKey(300)}
	stop := Entry{Key: makeKey(700)}
	tree.ScanRange(start, stop, func(e Entry) bool {
		if e.Key == "" && e.Key < start.Key && e.Key > stop.Key {
			t.Errorf("scan range, issue with key: %v", e.Key)
			return false
		}
		if printInfo {
			log.Printf("key: %s\n", e.Key)
		}
		return true
	})

	tree.Close()
}

// signature: ToList() (*list.List, error)
func TestRbTree_ToList(t *testing.T) {
	tree := NewRBTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())

	l, err := tree.ToList()
	if err != nil {
		t.Errorf("tolist: %v", err)
	}
	util.AssertLen(t, n*thousand, l.Len())
	l = nil
	tree.Close()
}

// signature: FromList(li *list.List) error
func TestRbTree_FromList(t *testing.T) {
	tree := NewRBTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	treeList, err := tree.ToList()
	if err != nil {
		t.Errorf("to list: %v", err)
	}
	util.AssertLen(t, n*thousand, treeList.Len())
	tree.Close()

	tree = NewRBTree()
	util.AssertLen(t, 0, tree.Len())

	err = tree.FromList(treeList)
	if err != nil {
		t.Errorf("from list: %v", err)
	}
	treeList = nil
	util.AssertLen(t, n*thousand, tree.Len())

	tree.Close()
}

// signature: Close()
func TestRbTree_Close(t *testing.T) {
	var tree *RBTree
	tree = NewRBTree()
	tree.Close()
}

func makeKey(i int) string {
	return fmt.Sprintf("key-%.6d", i)
}

func makeVal(i int) []byte {
	return []byte(fmt.Sprintf("{\"id\":%.6d,\"key\":\"key-%.6d\",\"value\":\"val-%.6d\"}", i, i, i))
}
