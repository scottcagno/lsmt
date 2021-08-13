package lsm

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type LogFile struct {
	file *os.File
	size int64
}

type entry struct {
	key string
	val string
}

func OpenLogFile(path string) (*LogFile, error) {
	fd, err := openOrCreate(path)
	if err != nil {
		return nil, fmt.Errorf("[OpenLogFile] opening: %v", err)
	}
	fi, err := fd.Stat()
	if err != nil {
		return nil, fmt.Errorf("[OpenLogFile] stat: %v", err)
	}
	return &LogFile{
		file: fd,
		size: fi.Size(),
	}, nil
}

func (l *LogFile) Append(data []byte) error {
	_, err := l.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("[LogFile.Write] seek: %v", err)
	}
	return nil
}

func (l *LogFile) WriteRecord(record *DataRecord) error {
	data, err := record.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = l.file.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func (l *LogFile) ReadRecord() (*DataRecord, error) {
	head := make([]byte, 8)
	_, err := l.file.Read(head)
	if err != nil {
		return nil, err
	}
	rlen := binary.LittleEndian.Uint64(head)
	data := make([]byte, rlen)
	_, err = l.file.Read(data)
	if err != nil {
		return nil, err
	}
	record := new(DataRecord)
	err = record.UnmarshalBinary(data)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (l *LogFile) WriteAdd(key string, value []byte) error {
	err := l.write(key, value)
	if err != nil {
		return fmt.Errorf("[LogFile.WriteAdd] calling write: %v", err)
	}
	return nil
}

func (l *LogFile) WritePut(key string, value []byte) error {
	err := l.write(key, value)
	if err != nil {
		return fmt.Errorf("[LogFile.WritePut] calling write: %v", err)
	}
	return nil
}

func (l *LogFile) WriteDel(key string) error {
	err := l.write(key, nil)
	if err != nil {
		return fmt.Errorf("[LogFile.WriteDel] calling write: %v", err)
	}
	return nil
}

func (l *LogFile) WriteEntry(e *entry) error {
	err := l.write(e.key, []byte(e.val))
	if err != nil {
		return fmt.Errorf("[LogFile.WriteEntry] calling write: %v", err)
	}
	return nil
}

func (l *LogFile) ReadEntry(e *entry) error {
	key, val, err := l.read()
	if err != nil {
		if err == io.EOF {
			return err
		}
		return fmt.Errorf("[LogFile.ReadEntry] calling read: %v", err)
	}
	e.key = key
	e.val = string(val)
	return nil
}

func (l *LogFile) Close() error {
	err := l.file.Sync()
	if err != nil {
		return fmt.Errorf("[LogFile.Close] calling file.Sync: %v", err)
	}
	err = l.file.Close()
	if err != nil {
		return fmt.Errorf("[LogFile.Close] calling file.Close: %v", err)
	}
	return nil
}

func (l *LogFile) read() (string, []byte, error) {
	key, val, err := readEntry(l.file)
	if err != nil {
		if err == io.EOF {
			return key, val, err
		}
		return key, val, fmt.Errorf("[LogFile.read] calling readEntry: %v", err)
	}
	return key, val, err
}

func (l *LogFile) write(key string, val []byte) error {
	err := writeEntry(l.file, key, val)
	if err != nil {
		return fmt.Errorf("[LogFile.write] calling writeEntry: %v", err)
	}
	return l.file.Sync()
}
