package rbtree

import (
	"github.com/scottcagno/leviathan/pkg/util"
	"testing"
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

}

// signature: Put(key string, val []byte) ([]byte, bool)
func TestRbTree_Put(t *testing.T) {

}

// signature: Get(key string) ([]byte, bool)
func TestRbTree_Get(t *testing.T) {

}

// signature: Del(key string) ([]byte, bool)
func TestRbTree_Del(t *testing.T) {

}

// signature: Len() int
func TestRbTree_Len(t *testing.T) {

}

// signature: Size() int64
func TestRbTree_Size(t *testing.T) {

}

// signature: Min() (Entry, bool)
func TestRbTree_Min(t *testing.T) {

}

// signature: Max() (Entry, bool)
func TestRbTree_Max(t *testing.T) {

}

// signature: ScanFront(iter Iterator)
func TestRbTree_ScanFront(t *testing.T) {

}

// signature: ScanBack(iter Iterator)
func TestRbTree_ScanBack(t *testing.T) {

}

// signature: ScanRange(start Entry, end Entry, iter Iterator)
func TestRbTree_ScanRange(t *testing.T) {

}

// signature: ToList() (*list.List, error)
func TestRbTree_ToList(t *testing.T) {

}

// signature: FromList(li *list.List) error
func TestRbTree_FromList(t *testing.T) {

}

// signature: Close()
func TestRbTree_Close(t *testing.T) {

}
