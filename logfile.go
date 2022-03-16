/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package lws

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"syscall"

	"chainmaker.org/chainmaker/lws/fbuffer"
)

type fileBuffer interface {
	Truncate(size int64) error
	Seek(offset int64, whence int) (int64, error)
	ReadAt(offset int64, n int) ([]byte, error)
	Next(n int) ([]byte, error)
	Close() error
	Size() int64
	WriteBack() error
}
type logfile struct {
	*os.File
	buf  fileBuffer
	sync func() error
}

func newLogFile(fn string, ft FileType, bufSize int) (*logfile, error) {
	f, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	var (
		fb   fileBuffer
		sync func() error = f.Sync
	)
	switch ft {
	case FT_NORMAL:
		if bufSize > 0 {
			fb, err = fbuffer.NewFixedBuffer(f, bufSize)
		}
	case FT_MMAP:
		if bufSize == 0 {
			return nil, errors.New("mmp size must greater than 0 for mmap file")
		}
		var buf *fbuffer.ZeroMmap
		buf, err = fbuffer.NewZeroMmap(f, bufSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED, false)
		sync = buf.Sync
		fb = buf
	}
	if err != nil {
		return nil, err
	}
	return &logfile{
		File: f,
		buf:  fb,
		sync: sync,
	}, nil
}

func (f *logfile) WriteLog(t int8, data []byte, crc32 uint32) (int, error) {
	if f.hasBuffer() {
		return f.writeWithBuffer(t, data, crc32)
	}
	return f.writeNoBuffer(t, data, crc32)
}

func (f *logfile) writeWithBuffer(t int8, data []byte, crc32 uint32) (int, error) {
	dl := len(data) + crc32Size + typeSize
	buf, err := f.buf.Next(dl + lenSize)
	if err != nil {
		return 0, err
	}
	serializateUint32(buf[:lenSize], uint32(dl))
	serializateUint32(buf[lenSize:], crc32)
	buf[lenSize+crc32Size] = byte(t)
	copy(buf[lenSize+crc32Size+1:], data)
	return len(buf), nil
}

func (f *logfile) writeNoBuffer(t int8, data []byte, crc32 uint32) (int, error) {
	dl := len(data) + crc32Size + typeSize
	buf := make([]byte, dl+lenSize)
	serializateUint32(buf[:lenSize], uint32(dl))
	serializateUint32(buf[lenSize:], crc32)
	buf[lenSize+crc32Size] = byte(t)
	copy(buf[lenSize+crc32Size+1:], data)
	return f.Write(buf)
}

func (f *logfile) hasBuffer() bool {
	return f.buf != nil
}

func (f *logfile) ReadLog(pos int64) (*LogEntry, error) {
	if f.hasBuffer() {
		return f.readWithBuffer(pos)
	}
	return f.readNoBuffer(pos)
}

func (f *logfile) readWithBuffer(pos int64) (*LogEntry, error) {
	lbz, err := f.buf.ReadAt(pos, lenSize)
	if err != nil {
		return nil, err
	}
	l := int(deserializeUint32(lbz))
	data, err := f.buf.ReadAt(pos+lenSize, l)
	if err != nil {
		return nil, err
	}
	c := deserializeUint32(data)
	return &LogEntry{
		Len:   l,
		Crc32: c,
		Typ:   int8(data[crc32Size]),
		Data:  data[crc32Size+typeSize:],
	}, nil
}

func (f *logfile) readNoBuffer(pos int64) (*LogEntry, error) {
	lbz := make([]byte, lenSize)
	_, err := f.File.ReadAt(lbz, pos)
	if err != nil && err != io.EOF {
		return nil, err
	}
	l := int(deserializeUint32(lbz))
	if l == 0 {
		return nil, nil
	}
	dbz := make([]byte, l)
	_, err = f.File.ReadAt(dbz, pos+lenSize)
	if err != nil && err != io.EOF {
		return nil, err
	}
	c := deserializeUint32(dbz)
	return &LogEntry{
		Len:   l,
		Crc32: c,
		Typ:   int8(dbz[crc32Size]),
		Data:  dbz[crc32Size+typeSize:],
	}, nil
}

func (f *logfile) WriteBack() error {
	if f.hasBuffer() {
		return f.buf.WriteBack()
	}
	return nil
}

func (f *logfile) Sync() error {
	if f.hasBuffer() {
		if err := f.buf.WriteBack(); err != nil {
			return err
		}
	}

	return f.sync()
}

func (f *logfile) Truncate(size int64) error {
	if f.hasBuffer() {
		if err := f.buf.Truncate(size); err != nil {
			return err
		}
	}
	return f.File.Truncate(size)
}

func (f *logfile) Seek(offset int64, whence int) (int64, error) {
	if f.hasBuffer() {
		if _, err := f.buf.Seek(offset, whence); err != nil {
			return -1, err
		}
		off, _ := f.buf.Seek(0, io.SeekCurrent)
		return f.File.Seek(off, io.SeekStart)
	}
	return f.File.Seek(offset, whence)
}

func (f *logfile) Close() error {
	if f.hasBuffer() {
		if err := f.buf.Close(); err != nil {
			return err
		}
	}
	return f.File.Close()
}

func (f *logfile) Size() int64 {
	if f.hasBuffer() {
		return f.buf.Size()
	}
	finfo, err := f.Stat()
	if err != nil {
		return -1
	}
	return finfo.Size()
}

func serializateUint32(b []byte, v uint32) {
	binary.BigEndian.PutUint32(b, v)
}

func deserializeUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}
