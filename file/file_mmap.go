/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package file

import (
	"errors"
	"io"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

var (
	OsPageSize     = os.Getpagesize()
	OsPageSizeMask = ^(OsPageSize - 1)
)

type MmapFile struct {
	f      *os.File //映射的文件
	fSize  int64    //文件大小
	offset int64    //读写偏移
	mmapInfo
}

type mmapInfo struct {
	mmOff  int64  //映射偏移
	mmSize int    //映射大小
	mmArea []byte //映射的区域
}

func NewMmapFile(path string, mmSize int) (*MmapFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()
	finfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fSize := finfo.Size()
	mmi, err := mmap(int(f.Fd()), 0, mmSize)

	return &MmapFile{
		f:        f,
		fSize:    fSize,
		mmapInfo: mmi,
	}, nil
}

func (mf *MmapFile) Truncate(size int64) error {
	err := mf.f.Truncate(size)
	if err != nil {
		return err
	}
	mf.fSize = size
	if mf.offset > size {
		mf.offset = size
	}
	return nil
}

func mmap(fd int, offset int64, size int) (_ mmapInfo, err error) {
	if size&^OsPageSizeMask != 0 {
		size = (size + OsPageSize) & OsPageSizeMask
	}
	offset = offset & int64(OsPageSizeMask)

	var (
		data []byte
	)
	data, err = syscall.Mmap(fd, offset, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return
	}
	return mmapInfo{
		mmArea: data,
		mmSize: size,
		mmOff:  offset,
	}, nil
}

func (mf *MmapFile) Seek(offset int64, whence int) (ret int64, err error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += mf.offset
	case io.SeekEnd:
		offset += mf.fSize
	}
	if offset < 0 {
		return 0, errors.New("seek offset invaild")
	}
	mf.offset = offset
	return mf.offset, nil
}

func (mf *MmapFile) remap(offset int64) error {
	var err error
	if mf.mmArea != nil {
		err = syscall.Munmap(mf.mmArea)
		if err != nil {
			return err
		}
		mf.mmArea = nil
	}
	mmi, err := mmap(int(mf.f.Fd()), offset, mf.mmSize)
	if err == nil {
		mf.mmArea = mmi.mmArea
		mf.mmOff = mmi.mmOff
	}
	return err
}

func (mf *MmapFile) Write(data []byte) (int, error) {
	var (
		writeN   int
		mmEnd    = mf.mmOff + int64(mf.mmSize)
		writeEnd = mf.offset + int64(len(data))
	)
	if writeEnd > mf.fSize {
		if err := syscall.Ftruncate(int(mf.f.Fd()), writeEnd); err != nil {
			return 0, err
		}
		mf.fSize = writeEnd
	}

	for len(data) > writeN {
		if mf.offset < mf.mmOff || mf.offset >= mmEnd || mf.mmArea == nil {
			if err := mf.remap(mf.offset); err != nil {
				return 0, syscall.EAGAIN
			}
			mmEnd = mf.mmOff + int64(mf.mmSize)
		}
		n := copy(mf.mmArea[mf.offset-mf.mmOff:], data[writeN:])
		mf.offset += int64(n)
		writeN += n
		if mf.offset > mf.fSize {
			mf.fSize = mf.offset
		}
	}
	return writeN, nil
}

func (mf *MmapFile) Sync() error {
	if mf.mmArea != nil {
		return unix.Msync(mf.mmArea, unix.MS_SYNC)
	}
	return nil
}

func (mf *MmapFile) Read(data []byte) (int, error) {
	var (
		readN   int
		mmEnd   = mf.mmOff + int64(mf.mmSize)
		readAll = mf.fSize - mf.offset
	)

	for mf.offset < mf.fSize && readN < cap(data) {
		if mf.offset < mf.mmOff || mf.offset >= mmEnd || mf.mmArea == nil {
			if err := mf.remap(mf.offset); err != nil {
				return readN, syscall.EAGAIN
			}
			mmEnd = mf.mmOff + int64(mf.mmSize)
		}
		n := copy(data[readN:], mf.mmArea[mf.offset-mf.mmOff:])
		mf.offset += int64(n)
		readN += n
	}

	if int64(readN) > readAll {
		readN = int(readAll)
	}

	if mf.offset >= mf.fSize {
		mf.offset = mf.fSize
		return readN, io.EOF
	}
	return readN, nil
}

func (mf *MmapFile) Close() error {
	if mf.f != nil {
		if err := mf.f.Close(); err != nil {
			return err
		}
	}
	if mf.mmArea != nil {
		return syscall.Munmap(mf.mmArea)
	}
	return nil
}

func (mf *MmapFile) Size() int64 {
	mf.f.Sync()
	info, err := mf.f.Stat()
	if err != nil {
		return -1
	}
	return info.Size()
}
