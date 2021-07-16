package rbtree

import "fmt"

// Entry represents a key value pair for the ordered map.
// If the user decides to change any of this, take care
// to ensure you implement the Item interface properly in
// order to keep everything in working order.
type Entry struct {
	Key   string
	Value []byte
}

func (e *Entry) Compare(that Entry) int {
	if len(e.Key) < len(that.Key) {
		return -1
	}
	if len(e.Key) > len(that.Key) {
		return +1
	}
	if e.Key < that.Key {
		return -1
	}
	if e.Key > that.Key {
		return 1
	}
	return 0
}

func (e Entry) String() string {
	return fmt.Sprintf("Entry.Key=%q, Entry.Value=%q\n", e.Key, e.Value)
}
