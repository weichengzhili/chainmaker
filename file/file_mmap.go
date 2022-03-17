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

const maxRW = 1 << 30

//concurrent operations are unsafe
type MmapFile struct {
	f        *os.File //映射的文件
	fSize    int64    //文件大小
	offset   int64    //读写偏移
	waitSync area
	mmapInfo
	mmapPort int
	mmapFlag int
	mmapLock bool
}

type mmapInfo struct {
	mmOff  int64  //映射偏移
	mmSize int    //映射大小
	mmArea []byte //映射的区域
}

type area struct {
	off int64
	len int
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
	finfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fSize := finfo.Size()
	mf := &MmapFile{
		f:        f,
		fSize:    fSize,
		mmapPort: fileFlagToMapPort(flag),
		mmapFlag: mapFlag,
		mmapInfo: mmapInfo{
			mmSize: mmSize,
		},
		mmapLock: lock,
	}
	if err = mf.remap(0); err != nil {
		return nil, err
	}

	return mf, nil
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

func mmap(fd int, offset int64, size int, port int, flag int) (_ mmapInfo, err error) {
	size = int(alignUp(uint64(size), uint64(OsPageSize)))
	offset = int64(alignDown(uint64(offset), uint64(OsPageSize)))

	var (
		data []byte
	)
	data, err = syscall.Mmap(fd, offset, size, port, flag)
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
		return 0, errors.New(strSeekOffInvaild)
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
	mmi, err := mmap(int(mf.f.Fd()), offset, mf.mmSize, mf.mmapPort, mf.mmapFlag)
	if err == nil {
		if mf.mmapLock {
			syscall.Mlock(mmi.mmArea)
		}
		mf.mmArea = mmi.mmArea
		mf.mmOff = mmi.mmOff
	}
	return err
}

//concurrent operations are unsafe
func (mf *MmapFile) WriteAt(data []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, errors.New(strNegativeOffset)
	}
	return mf.writeAt(data, offset)
}

func (mf *MmapFile) shouldRemap(offset, end int64) bool {
	return offset < mf.mmOff || offset >= end || mf.mmArea == nil
}

func (mf *MmapFile) writeAt(data []byte, offset int64) (int, error) {
	var (
		writeN   int
		mmEnd    = mf.mmOff + int64(mf.mmSize)
		writeEnd = offset + int64(len(data))
	)
	if writeEnd > mf.fSize {
		if err := syscall.Ftruncate(int(mf.f.Fd()), writeEnd); err != nil {
			return 0, err
		}
		mf.fSize = writeEnd
	}

	for {
		if mf.shouldRemap(offset, mmEnd) {
			if err := mf.remap(offset); err != nil {
				mf.waitSync = mergeArea(mf.waitSync, area{
					off: offset - int64(writeN),
					len: writeN,
				})
				return writeN, syscall.EAGAIN
			}
			mmEnd = mf.mmOff + int64(mf.mmSize)
		}
		max := len(data)
		if max-writeN > maxRW {
			max = writeN + maxRW
		}
		n := copy(mf.mmArea[offset-mf.mmOff:], data[writeN:max])
		offset += int64(n)
		writeN += n
		if writeN >= len(data) {
			mf.waitSync = mergeArea(mf.waitSync, area{
				off: offset - int64(writeN),
				len: writeN,
			})
			return writeN, nil
		}
	}
}

//concurrent operations are unsafe
func (mf *MmapFile) Write(data []byte) (int, error) {
	writeN, err := mf.writeAt(data, mf.offset)
	mf.offset += int64(writeN)
	return writeN, err
}

func (mf *MmapFile) Sync() error {
	overlap := overlapArea(mf.waitSync, area{
		off: mf.mmOff,
		len: mf.mmSize,
	})

	if mf.mmArea != nil && overlap.len > 0 {
		off := int64(alignDown(uint64(overlap.off), uint64(OsPageSize)))
		overlap = area{
			off: off,
			len: int(overlap.off-off) + overlap.len,
		}
		if err := unix.Msync(mf.mmArea[overlap.off:overlap.off+int64(overlap.len)], unix.MS_SYNC); err != nil {
			return err
		}
		mf.waitSync = area{
			off: overlap.off + int64(overlap.len),
		}
		return nil
	}

	return nil
}

//concurrent operations are unsafe
func (mf *MmapFile) ReadAt(data []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, errors.New(strNegativeOffset)
	}
	if offset >= mf.fSize {
		return 0, io.EOF
	}
	return mf.readAt(data, offset)
}

func (mf *MmapFile) readAt(data []byte, offset int64) (int, error) {
	var (
		readN   int
		mmEnd   = mf.mmOff + int64(mf.mmSize)
		readAll = mf.fSize - offset
	)
	if int64(len(data)) > readAll {
		data = data[:readAll]
	}

	for offset < mf.fSize && readN < cap(data) {
		if mf.shouldRemap(offset, mmEnd) {
			if err := mf.remap(offset); err != nil {
				return readN, syscall.EAGAIN
			}
			mmEnd = mf.mmOff + int64(mf.mmSize)
		}
		n := copy(data[readN:], mf.mmArea[offset-mf.mmOff:])
		offset += int64(n)
		readN += n
	}

	if int64(readN) >= readAll {
		readN = int(readAll)
		return readN, io.EOF
	}

	return readN, nil
}

//concurrent operations are unsafe
func (mf *MmapFile) Read(data []byte) (int, error) {
	readN, err := mf.readAt(data, mf.offset)
	mf.offset += int64(readN)
	return readN, err
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

func mergeArea(a area, b area) area {
	aEnd := a.off + int64(a.len)
	bEnd := b.off + int64(b.len)
	if a.off <= b.off && aEnd >= bEnd {
		return a
	}
	if a.off > b.off && aEnd < bEnd {
		return b
	}
	if bEnd < a.off {
		return area{
			off: b.off,
			len: int(aEnd - b.off),
		}
	} else if b.off > aEnd {
		return area{
			off: a.off,
			len: int(bEnd - a.off),
		}
	} else if a.off < b.off {
		a.off = b.off
	} else {
		a.len = b.len
	}
	return a
}

func overlapArea(a area, b area) area {
	aEnd := a.off + int64(a.len)
	bEnd := b.off + int64(b.len)
	if a.off <= b.off && aEnd >= bEnd {
		return b
	}
	if a.off > b.off && aEnd < bEnd {
		return a
	}
	if bEnd < a.off || b.off > aEnd {
		return area{}
	} else if a.off < b.off {
		return area{
			off: b.off,
			len: int(bEnd - a.off),
		}
	} else {
		return area{
			off: a.off,
			len: int(aEnd - b.off),
		}
	}
}

func alignUp(n, a uint64) uint64 {
	return (n + a - 1) &^ (a - 1)
}

func alignDown(n, a uint64) uint64 {
	return n &^ (a - 1)
}
