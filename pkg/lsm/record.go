package lsm

import (
	"encoding/binary"
	"fmt"
)

const hlen = 12 // header length

type Record struct {
	data []byte
}

func NewRecord(key string, value []byte) *Record {
	klen, vlen := len(key), len(value)
	data := make([]byte, hlen+klen+vlen)
	binary.LittleEndian.PutUint32(data[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint64(data[4:12], uint64(len(value)))
	copy(data[hlen:hlen+klen], key)
	copy(data[hlen+klen:], value)
	return &Record{
		data: data,
	}
}

func (r *Record) KeyLen() uint32 {
	return binary.LittleEndian.Uint32(r.data[0:4])
}

func (r *Record) Key() []byte {
	klen := binary.LittleEndian.Uint32(r.data[0:4])
	data := make([]byte, klen)
	copy(data, r.data[hlen:hlen+klen])
	return data
}

func (r *Record) ValLen() uint64 {
	return binary.LittleEndian.Uint64(r.data[4:12])
}

func (r *Record) Value() []byte {
	klen := uint64(binary.LittleEndian.Uint32(r.data[0:4]))
	vlen := binary.LittleEndian.Uint64(r.data[4:12])
	data := make([]byte, vlen)
	copy(data, r.data[hlen+klen:hlen+klen+vlen])
	return data
}

func (r *Record) Size() uint64 {
	klen := uint64(binary.LittleEndian.Uint32(r.data[0:4]))
	vlen := binary.LittleEndian.Uint64(r.data[4:12])
	return klen + vlen
}

func (r *Record) String() string {
	klen := uint64(binary.LittleEndian.Uint32(r.data[0:4]))
	key := r.data[hlen : hlen+klen]
	vlen := binary.LittleEndian.Uint64(r.data[4:12])
	val := r.data[hlen+klen : hlen+klen+vlen]
	return fmt.Sprintf("klen=%d, vlen=%d, size=%d, key=%q, val=%q\n", klen, vlen, klen+vlen, key, val)
}
