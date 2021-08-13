package main

import (
	"github.com/scottcagno/lsmt/pkg/lsm"
	"log"
	"os"
)

func main() {

	mem, err := lsm.NewMemtable("cmd/memtable/log/memtable.log", true)

	//testPut(mem)
	//testHas(mem)
	//testGet(mem)

	err = mem.Close()
	errCheck(err)
}

func testRecord() {
	rec := lsm.NewRecord("foo", []byte("bar,baz"))
	log.Printf("record=%s", rec.String())
}

var (
	key0 = "testkey-000000"
	key1 = "testkey-000001"
	key2 = "testkey-000002"
	key3 = "testkey-000003"
	key4 = "testkey-000004"
	key5 = "testkey-000005"
	key6 = "testkey-000006"
	key7 = "testkey-000007"
	key8 = "testkey-000008"
	key9 = "testkey-000009"

	val0 = []byte("this is my zeroth test value, yeah #0")
	val1 = []byte("this is my first test value, yeah #1")
	val2 = []byte("this is my second test value, yeah #2")
	val3 = []byte("this is my third test value, yeah #3")
	val4 = []byte("this is my fourth test value, yeah #4")
	val5 = []byte("this is my fifth test value, yeah #5")
	val6 = []byte("this is my sixth test value, yeah #6")
	val7 = []byte("this is my seventh test value, yeah #7")
	val8 = []byte("this is my eighth test value, yeah #8")
	val9 = []byte("this is my ninth test value, yeah #9")
)

func testHas(mem *lsm.Memtable) {

	log.Printf("has %q -> %v\n", "foo", mem.Has("foo"))
	log.Printf("has %q -> %v\n", "bar", mem.Has("bar"))

	log.Printf("has %q -> %v\n", key0, mem.Has(key0))
	log.Printf("has %q -> %v\n", key1, mem.Has(key1))
	log.Printf("has %q -> %v\n", key2, mem.Has(key2))
	log.Printf("has %q -> %v\n", key3, mem.Has(key3))
	log.Printf("has %q -> %v\n", key4, mem.Has(key4))
	log.Printf("has %q -> %v\n", key5, mem.Has(key5))
	log.Printf("has %q -> %v\n", key6, mem.Has(key6))
	log.Printf("has %q -> %v\n", key7, mem.Has(key7))
	log.Printf("has %q -> %v\n", key8, mem.Has(key8))
	log.Printf("has %q -> %v\n", key9, mem.Has(key9))
}

func testPut(mem *lsm.Memtable) {
	var err error
	err = mem.Put(key0, val0)
	errCheck(err)
	err = mem.Put(key1, val1)
	errCheck(err)
	err = mem.Put(key2, val2)
	errCheck(err)
	err = mem.Put(key3, val3)
	errCheck(err)
	err = mem.Put(key4, val4)
	errCheck(err)
	err = mem.Put(key5, val5)
	errCheck(err)
	err = mem.Put(key6, val6)
	errCheck(err)
	err = mem.Put(key7, val7)
	errCheck(err)
	err = mem.Put(key8, val8)
	errCheck(err)
	err = mem.Put(key9, val9)
	errCheck(err)
}

func testGet(mem *lsm.Memtable) {
	var err error
	val0, err = mem.Get(key0)
	errAndValueCheck(err, key0, val0)
	val1, err = mem.Get(key1)
	errAndValueCheck(err, key1, val1)
	val2, err = mem.Get(key2)
	errAndValueCheck(err, key2, val2)
	val3, err = mem.Get(key3)
	errAndValueCheck(err, key3, val3)
	val4, err = mem.Get(key4)
	errAndValueCheck(err, key4, val4)
	val5, err = mem.Get(key5)
	errAndValueCheck(err, key5, val5)
	val6, err = mem.Get(key6)
	errAndValueCheck(err, key6, val6)
	val7, err = mem.Get(key7)
	errAndValueCheck(err, key7, val7)
	val8, err = mem.Get(key8)
	errAndValueCheck(err, key8, val8)
	val9, err = mem.Get(key9)
	errAndValueCheck(err, key9, val9)
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

func errAndValueCheck(err error, key string, val []byte) {
	if err != nil {
		log.Panic(err)
	}
	if val == nil {
		log.Panicf("got val for key(%s): %s\n", key, val)
	}
}
