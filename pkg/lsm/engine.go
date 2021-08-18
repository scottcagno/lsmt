package lsm

//import "github.com/scottcagno/db/lsmt"
//import "fmt"

func test() {

    // opens a new or existing db
    db, err := lsmt.Open()
    if err != nil {
        fmt.Panicf("open: %v", err)
    }
    
    myval := []byte("this is my value")
    
    // writes a key value pair to the
    // store, overwriting any existing
    // entry for the key
    err = db.Put("mykey", myval)
    if err != nil {
        fmt.Panicf("put: %v", err)
    }
    
    // returns true if the key exists
    // in the store
    ok := db.Has("mykey")
    if !ok {
        fmt.Panicf("has: 'mykey', %v", ok)
    }
    
    // returns the value associated with
    // key, or nil if no mapping exists
    val, err := db.Get("mykey")
    if err != nil || val == nil {
        fmt.Panicf("get: %v", err)
    }
    
    // returns the entry associated with
    // the givsn key, or nil/err if no
    // mapping exists
    ent, err := db.GetEntry("mykey")
    if err != nil {
        fmt.Panicf("getentry: %v", err)
    }
                         
    // removes the mapping for the key,
    // returns previous value if not null
    prev, err := db.Del("mykey")
    if err != nil || prev == nil {
        fmt.Panicf("del: %v", err)
    }
    
    // returns the first entry with a key
    // lower than or equal to key specified
    // or nil if the entry does not exist
    ent, err := db.Lower("mykey")
    if err != nil {
        fmt.Panicf("lower: %v", err)
    }
    
    // returns the first entry with a key
    // higher than or equal to key specified
    // or nil if the entry does not exist
    ent, err = db.Higher("mykey")
    if err != nil {
        fmt.Panicf("higher: %v", err)
    }
    
    // returns the first (lowest) entry,    
    // or nil if entry does not exist
    ent, err = db.First()
    if err != nil {
        fmt.Panicf("first: %v", err)
    }
    
    // returns the last (highest) entry, 
    // or nil if entry does not exist
    ent, err = db.Last()
    if err != nil {
        fmt.Panicf("last: %v", err)
    }
    
    // returns number of unique entries
    n, err = db.Count()
    if err != nil || n < 0 {
        fmt.Panicf("count: %v", err)
    }
    
    // iterates while condition is true
    // note: consider using entry over kv?
    db.Iter(func(k string, v []byte) bool {
       if k != "" {
           fmt.Printf("key: %q\n", k)
           return true
       }
       return false
    })
    
    // flushes volatile generation of data
    err = db.Flush()
    if err != nil {
        fmt.Panicf("flush: %v", err)
    }
    
    // calls a flush and closes the db
    err = db.Close()
    if err != nil {
        fmt.Panicf("close: %v", err)
    }

}

type Engine interface {

    // writes a key value pair to the
    // store, overwriting any existing
    // entry for the key
    Put(k string, v []byte) error
    
    // returns true if the key exists
    // in the store
    Has(k string) bool
    
    // returns the value associated with
    // key, or nil/err if no mapping exists
    Get(k string) ([]byte, error)
    
    // returns the entry associated with
    // key, or nil/err if no mapping exists
    GetEntry(k string) (*Entry, error)
                         
    // removes the mapping for the key,
    // returns previous value if not nil
    Del(k string) ([]byte, error)
    
    // returns the first entry with a key
    // lower than or equal to key specified
    // or nil/err if entry does not exist
    Lower(k string) (*Entry, error)
    
    // returns the first entry with a key
    // higher than or equal to key specified
    // or nil/err if entry does not exist
    Higher(k string) (*Entry, error)
    
    // returns the first (lowest) entry,    
    // or nil/err if entry does not exist
    First() (*Entry, error)
    
    // returns the last (highest) entry, 
    // or nil/err if entry does not exist
    Last() (*Entry, error)
    
    // returns number of unique entries
    Count() (int64, error)
    
    // iterates while condition is true
    Iter(it func(k string, v []byte) bool)
    
    // flushes volatile generation of data
    Flush() error
    
    // calls a flush and closes the store
    Close() error
}
