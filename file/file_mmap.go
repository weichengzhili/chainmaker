/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package file

import (
	"io"
	"os"
	"syscall"

	"chainmaker.org/chainmaker/lws/fbuffer"
	"github.com/pkg/errors"
)

const maxRW = 1 << 30

//concurrent operations are unsafe
type MmapFile struct {
	f      *os.File //映射的文件
	buf    *fbuffer.ZeroMmap
	offset int64
}

func NewMmapFile(path string, mmSize int) (*MmapFile, error) {
	return OpenMmapFile(path, mmSize, os.O_RDWR|os.O_CREATE, 0644, syscall.MAP_SHARED, false)
}

func OpenMmapFile(path string, mmSize int, flag int, perm os.FileMode, mapFlag int, lock bool) (*MmapFile, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()
	buf, err := fbuffer.NewZeroMmap(f, mmSize, fileFlagToMapPort(flag), mapFlag, lock)
	if err != nil {
		return nil, err
	}

	return &MmapFile{
		buf: buf,
		f:   f,
	}, nil
}

func fileFlagToMapPort(flag int) int {
	port := syscall.PROT_READ
	if flag&os.O_WRONLY == os.O_WRONLY {
		port = syscall.PROT_WRITE
	}
	if flag&os.O_RDWR == os.O_RDWR {
		port |= syscall.PROT_READ | syscall.PROT_WRITE
	}
	return port
}

func (mf *MmapFile) Truncate(size int64) error {
	if err := mf.f.Truncate(size); err != nil {
		return err
	}
	if mf.offset > size {
		mf.offset = size
	}

	return mf.buf.Truncate(size)
}

func (mf *MmapFile) Seek(offset int64, whence int) (ret int64, err error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += mf.offset
	case io.SeekEnd:
		s := mf.Size()
		if s < 0 {
			return mf.offset, errors.WithMessage(syscall.EAGAIN, "MmapFile-Seek")
		}
		offset += s
	}
	if offset < 0 {
		return 0, errors.New(strSeekOffInvaild)
	}
	mf.offset = offset
	return mf.offset, nil
}

//concurrent operations are unsafe
func (mf *MmapFile) WriteAt(data []byte, offset int64) (int, error) {
	b, err := mf.buf.NextAt(offset, len(data))
	if err != nil {
		return 0, err
	}
	return copy(b, data), nil
}

//concurrent operations are unsafe
func (mf *MmapFile) Write(data []byte) (int, error) {
	b, err := mf.buf.NextAt(mf.offset, len(data))
	if err != nil {
		return 0, err
	}
	mf.offset += int64(len(b))
	return copy(b, data), nil
}

func (mf *MmapFile) Sync() error {
	return mf.buf.Sync()
}

//concurrent operations are unsafe
func (mf *MmapFile) ReadAt(data []byte, offset int64) (int, error) {
	b, err := mf.buf.ReadAt(offset, len(data))
	if err != nil {
		return 0, err
	}
	return copy(data, b), nil
}

//concurrent operations are unsafe
func (mf *MmapFile) Read(data []byte) (int, error) {
	b, err := mf.buf.ReadAt(mf.offset, len(data))
	if err != nil {
		return 0, err
	}
	mf.offset += int64(len(b))
	return copy(data, b), nil
}

func (mf *MmapFile) Close() error {
	if mf.buf != nil {
		if err := mf.buf.Close(); err != nil {
			return err
		}
		mf.buf = nil
	}
	if mf.f != nil {
		if err := mf.f.Close(); err != nil {
			return err
		}
		mf.f = nil
	}
	return nil
}

func (mf *MmapFile) Size() int64 {
	info, err := mf.f.Stat()
	if err != nil {
		return -1
	}
	return info.Size()
}
