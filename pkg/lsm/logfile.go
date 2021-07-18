package lsm

import (
	"fmt"
	"io"
	"os"
)

type LogFile struct {
	file *os.File
	size int64
}

func OpenLogFile(path string) (*LogFile, error) {
	fd, err := OpenOrCreate(path)
	if err != nil {
		return nil, fmt.Errorf("[OpenLogFile] opening: %v", err)
	}
	fi, err := fd.Stat()
	if err != nil {
		return nil, fmt.Errorf("[OpenLogFile] stat: %v", err)
	}
	_, err = fd.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("[OpenLogFile] seek: %v", err)
	}
	return &LogFile{
		file: fd,
		size: fi.Size(),
	}, nil
}

func (l *LogFile) read() (byte, string, []byte, error) {
	typ, key, val, err := readEntry(l.file)
	if err != nil {
		return typ, key, val, fmt.Errorf("[LogFile.read] calling readEntry: %v", err)
	}
	return typ, key, val, err
}

func (l *LogFile) write(typ byte, key string, val []byte) error {
	err := writeEntry(l.file, typ, key, val)
	if err != nil {
		return fmt.Errorf("[LogFile.write] calling writeEntry: %v", err)
	}
	return l.file.Sync()
}

func (l *LogFile) WriteAdd(key string, value []byte) error {
	err := l.write(typeAdd, key, value)
	if err != nil {
		return fmt.Errorf("[LogFile.WriteAdd] calling write: %v", err)
	}
	return nil
}

func (l *LogFile) WritePut(key string, value []byte) error {
	err := l.write(typePut, key, value)
	if err != nil {
		return fmt.Errorf("[LogFile.WritePut] calling write: %v", err)
	}
	return nil
}

func (l *LogFile) WriteDel(key string) error {
	err := l.write(typeDel, key, nil)
	if err != nil {
		return fmt.Errorf("[LogFile.WriteDel] calling write: %v", err)
	}
	return nil
}

func (l *LogFile) WriteEntry(e *entry) error {
	err := l.write(e.typ, e.key, []byte(e.val))
	if err != nil {
		return fmt.Errorf("[LogFile.WriteEntry] calling write: %v", err)
	}
	return nil
}

func (l *LogFile) ReadEntry(e *entry) error {
	typ, key, val, err := l.read()
	if err != nil {
		return fmt.Errorf("[LogFile.ReadEntry] calling read: %v", err)
	}
	e.typ = typ
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
