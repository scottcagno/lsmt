package lsm

import (
	"fmt"
	"github.com/scottcagno/lsmt/pkg/lsm/rbtree"
	"io"
	"os"
	"sync"
	"time"
)

type Memtable struct {
	mu        sync.RWMutex   // lock
	data      *rbtree.RBTree // memtable sorted index
	aol       *LogFile       // log file for crashes
	threshold int64          // sstable flush threshold
	size      int64          // size in bytes (used to check if threshold has been met)
}

func NewMemtable(path string) (*Memtable, error) {
	l, err := OpenLogFile(path)
	if err != nil {
		return nil, fmt.Errorf("[NewMemtable] calling OpenLogFile: %v", err)
	}
	m := &Memtable{
		data: rbtree.NewRBTree(),
		aol:  l,
		size: l.size,
	}
	m.load()
	return m, nil
}

func (m *Memtable) load() {
	m.mu.Lock()
	defer m.mu.Unlock()
	var err error
	var entries []entry
	if m.size > 0 {
		for err != io.EOF {
			var e entry
			err = m.aol.ReadEntry(&e)
			switch e.typ {
			case typeErr, typeDel:
				continue
			case typeAdd, typePut:
				entries = append(entries, e)
				//m.data.Put(e.key, e.val)
			}
		}
	}
	fmt.Printf(">> entries: %+v\n", entries)
}

func (m *Memtable) Put(key string, val []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// write put entry to the logile
	err := m.aol.WritePut(key, val)
	if err != nil {
		return fmt.Errorf("[Memtable.Put] calling WritePut: %v", err)
	}

	// add entry to the memtable
	m.data.Put(key, val)

	// update size (if need be)
	s := m.data.Size()
	if s > m.size {
		m.size += s
	}

	return nil
}

func (m *Memtable) Get(key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// check the memtable
	val, ok := m.data.Get(key)
	if !ok {
		return nil, ErrNotFound
	}
	return val, nil
}

func (m *Memtable) Del(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// write del entry to the logfile
	err := m.aol.WriteDel(key)
	if err != nil {
		return fmt.Errorf("[Memtable.Del] calling WriteDel: %v", err)
	}

	// remove entry from the memtable
	m.data.Del(key)
	s := m.data.Size()

	// update size (if need be)
	if s < m.size {
		m.size -= s
	}
	return nil
}

func (m *Memtable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

func (m *Memtable) ShouldFlush() bool {
	if m.size > m.threshold-m.threshold/10 {
		return true
	}
	return false
}

func (m *Memtable) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// create new sstable file
	filename := fmt.Sprintf("dat-%d.sst", time.Now().Unix())
	fd, err := OpenOrCreate(filename)
	if err != nil {
		return fmt.Errorf("[Memtable.Flush] calling OpenOrCreate: %v", err)
	}
	defer fd.Close()

	// iterate all of the entries in the memtable in order
	m.data.ScanFront(func(e rbtree.Entry) bool {
		// write each entry to the sstable file
		err = writeEntry(fd, typeAdd, e.Key, e.Value)
		if err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return fmt.Errorf("[Memtable.Flush] scanning front (i think): %v", err)
	}
	// make sure file is flushed to disk
	err = fd.Sync()
	if err != nil {
		return fmt.Errorf("[Memtable.Flush] calling fd.Sync: %v", err)
	}

	// reset the memtable data
	m.data.Close()
	m.data = rbtree.NewRBTree()

	// get the log file name
	path := m.aol.file.Name()

	// close and remove the existing log file
	// we don't need this one anymore
	err = m.aol.Close()
	if err != nil {
		return fmt.Errorf("[Memtable.Flush] calling fd.Close: %v", err)
	}
	err = os.Remove(path)
	if err != nil {
		return fmt.Errorf("[Memtable.Flush] calling os.Remove: %v", err)
	}

	// open a fresh log file
	l, err := OpenLogFile(path)
	if err != nil {
		return fmt.Errorf("[Memtable.Flush] calling OpenLogFile: %v", err)
	}
	m.aol = l

	// reset the memtable size
	m.size = 0
	return nil
}

func (m *Memtable) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.aol.Close()
	if err != nil {
		return fmt.Errorf("[Memtable.Close] calling fd.Close: %v", err)
	}
	m.data.Close()
	return nil
}
