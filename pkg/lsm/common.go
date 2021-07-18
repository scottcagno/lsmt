package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	typeErr = 0x00 // error type marker
	typeAdd = 0xf0 // insert type marker
	typePut = 0xf1 // update type marker
	typeDel = 0xf2 // delete type marker
)

var ErrNotFound = errors.New("not found")

type entry struct {
	typ byte
	key string
	val string
}

func readEntry(r io.Reader) (byte, string, []byte, error) {
	header := make([]byte, 13)

	_, err := r.Read(header)
	if err != nil {
		return typeErr, "", nil, fmt.Errorf("[readEntry] reading header: %v", err)
	}

	typ := header[0]
	keylen := binary.LittleEndian.Uint32(header[1:4])
	vallen := binary.LittleEndian.Uint64(header[4:12])

	data := make([]byte, uint64(keylen)+vallen)
	_, err = r.Read(data)
	if err != nil {
		return typeErr, "", nil, fmt.Errorf("[readEntry] reading data: %v", err)
	}

	return typ, string(data[:keylen]), data[keylen : uint64(keylen)+vallen], nil
}

func writeEntry(w io.Writer, typ byte, key string, val []byte) error {

	// write type
	_, err := w.Write([]byte{typ})
	if err != nil {
		return fmt.Errorf("[writeEntry] writing type: %v", err)
	}

	// write keylen
	var keylen [4]byte
	binary.LittleEndian.PutUint32(keylen[:], uint32(len(key)))
	_, err = w.Write(keylen[:])
	if err != nil {
		return fmt.Errorf("[writeEntry] writing key length: %v", err)
	}

	// write vallen
	var vallen [8]byte
	binary.LittleEndian.PutUint64(vallen[:], uint64(len(val)))
	_, err = w.Write(vallen[:])
	if err != nil {
		return fmt.Errorf("[writeEntry] writing value length: %v", err)
	}

	// write key data
	_, err = w.Write([]byte(key))
	if err != nil {
		return fmt.Errorf("[writeEntry] writing key data: %v", err)
	}

	// write val data
	_, err = w.Write(val)
	if err != nil {
		return fmt.Errorf("[writeEntry] writing value data: %v", err)
	}

	return nil
}

const filePermissions = 0600 //| os.ModeSticky

func OpenOrCreate(path string) (*os.File, error) {
	// sanitize path
	path, err := filepath.Abs(filepath.Clean(filepath.ToSlash(path)))
	if err != nil {
		return nil, fmt.Errorf("[OpenOrCreate] sanitizing path: %v", err)
	}
	// check to see if we need to create a new file
	if _, err := os.Stat(path); err != nil && os.IsNotExist(err) {
		// sanitize the filepath
		dirs, _ := filepath.Split(path)
		// create any directories
		if err := os.MkdirAll(dirs, os.ModeDir); err != nil {
			return nil, fmt.Errorf("[OpenOrCreate] making dirs: %v", err)
		}
		// create the new file
		fd, err := os.Create(path)
		if err != nil {
			return nil, fmt.Errorf("[OpenOrCreate] creating file: %v", err)
		}
		// close the file
		if err = fd.Close(); err != nil {
			return nil, fmt.Errorf("[OpenOrCreate] closing file: %v", err)
		}
	}
	// already existing
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, filePermissions) // perm: 0600|os.ModeSticky
	if err != nil {
		return nil, fmt.Errorf("[OpenOrCreate] opening file: %v", err)
	}
	return fd, nil
}
