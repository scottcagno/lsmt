package main

import (
	"github.com/scottcagno/lsmt/pkg/lsm"
	"log"
	"os"
)

func main() {

	mem, err := lsm.NewMemtable("cmd/memtable/log/memtable.log")
	errCheck(err)
	defer mem.Close()

	testPut(mem)

	err = mem.Close()
	errCheck(err)
}

func testPut(mem *lsm.Memtable) {
	var err error

	err = mem.Put("testkey-1", []byte("this is my first test value, yeah #1"))
	errCheck(err)

	err = mem.Put("testkey-2", []byte("this is my second test value, yeah #2"))
	errCheck(err)

	err = mem.Put("testkey-3", []byte("this is my third test value, yeah #3"))
	errCheck(err)
}

func cleanup(path string) {
	err := os.RemoveAll(path)
	if err != nil {
		log.Panic(err)
	}
}

func errCheck(err error) {
	if err != nil {
		log.Panic(err)
	}
}
