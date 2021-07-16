package lsm

import (
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
		return nil, err
	}
	fi, err := fd.Stat()
	if err != nil {
		return nil, err
	}
	_, err = fd.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	return &LogFile{
		file: fd,
		size: fi.Size(),
	}, nil
}

func (l *LogFile) read() (byte, string, []byte, error) {
	return readEntry(l.file)
}

func (l *LogFile) write(typ byte, key string, val []byte) error {
	err := writeEntry(l.file, typ, key, val)
	if err != nil {
		return err
	}
	return l.file.Sync()
}

func (l *LogFile) WriteAdd(key string, value []byte) error {
	return l.write(typeAdd, key, value)
}

func (l *LogFile) WritePut(key string, value []byte) error {
	return l.write(typePut, key, value)
}

func (l *LogFile) WriteDel(key string) error {
	return l.write(typeDel, key, nil)
}

func (l *LogFile) WriteEntry(e *entry) error {
	return l.write(e.typ, e.key, []byte(e.val))
}

func (l *LogFile) ReadEntry(e *entry) error {
	typ, key, val, err := l.read()
	if err != nil {
		return err
	}
	e.typ = typ
	e.key = key
	e.val = string(val)
	return nil
}

func (l *LogFile) Close() error {
	err := l.file.Sync()
	if err != nil {
		return err
	}
	err = l.file.Close()
	if err != nil {
		return err
	}
	return nil
}
