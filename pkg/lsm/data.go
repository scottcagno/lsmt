package lsm

import (
	"encoding/binary"
	"time"
)

type DataRecord struct {
	Meta      uint64 // not used right now, but here for future use
	Timestamp time.Time
	Key       string
	Value     []byte
}

func NewDataRecord(key string, value []byte) *DataRecord {
	return &DataRecord{
		Meta:      0,
		Timestamp: time.Now(),
		Key:       key,
		Value:     value,
	}
}

func (d *DataRecord) UpdateTimestamp() {
	d.Timestamp = time.Now()
}

func (d *DataRecord) MarshalBinary() ([]byte, error) {
	data := make([]byte, 32+len(d.Key)+len(d.Value))
	binary.LittleEndian.PutUint64(data[0:8], d.Meta)
	binary.LittleEndian.PutUint64(data[8:16], uint64(d.Timestamp.Unix()))
	binary.LittleEndian.PutUint64(data[16:24], uint64(len(d.Key)))
	binary.LittleEndian.PutUint64(data[24:32], uint64(len(d.Value)))
	copy(data[32:32+len(d.Key)], d.Key)
	copy(data[32+len(d.Key):32+len(d.Key)+len(d.Value)], d.Value)
	return data, nil
}

func (d *DataRecord) UnmarshalBinary(data []byte) error {
	d.Meta = binary.LittleEndian.Uint64(data[0:8])
	d.Timestamp = time.Unix(int64(binary.LittleEndian.Uint64(data[8:16])), 0)
	klen := binary.LittleEndian.Uint64(data[16:24])
	vlen := binary.LittleEndian.Uint64(data[24:32])
	d.Key = string(data[32 : 32+klen])
	d.Value = data[32+klen : 32+klen+vlen]
	return nil
}
