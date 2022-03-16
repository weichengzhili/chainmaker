/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package fbuffer

import (
	"errors"
	"io"
	"os"
	"syscall"

	"chainmaker.org/chainmaker/lws/allocate"
	"golang.org/x/sys/unix"
)

var (
	OsPageSize        = os.Getpagesize()
	strNegativeOffset = "negative offset"
	strSeekOffInvaild = "seek offset invaild"
	strInvaildArg     = "arguments invaild"
)

//concurrent operations are unsafe
type ZeroMmap struct {
	f         *os.File //映射的文件
	fSize     int64    //文件大小
	offset    int64    //读写偏移
	waitSync  area
	mmSize    int
	mmOff     int64
	allocator *allocate.MmapAllocator
}
type area struct {
	off int64
	len int
}

// func OpenZeroMmap(path string, mmSize int, flag int, perm os.FileMode, mapFlag int, lock bool) (*ZeroMmap, error) {
// 	f, err := os.OpenFile(path, flag, perm)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer func() {
// 		if err != nil {
// 			f.Close()
// 		}
// 	}()
// 	finfo, err := f.Stat()
// 	if err != nil {
// 		return nil, err
// 	}
// 	allocator, err := allocate.NewMmapAllocator(f, 0, mmSize, fileFlagToMapPort(flag), mapFlag, lock)
// 	if err != nil {
// 		return nil, err
// 	}
// 	mf := &ZeroMmap{
// 		f:         f,
// 		fSize:     finfo.Size(),
// 		mmSize:    mmSize,
// 		allocator: allocator,
// 	}

// 	return mf, nil
// }

func NewZeroMmap(f *os.File, mmSize int, mapPort, mapFlag int, lock bool) (*ZeroMmap, error) {
	finfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	allocator, err := allocate.NewMmapAllocator(f, 0, mmSize, mapPort, mapFlag, lock)
	if err != nil {
		return nil, err
	}
	return &ZeroMmap{
		f:         f,
		fSize:     finfo.Size(),
		mmSize:    mmSize,
		allocator: allocator,
	}, nil
}

func (zm *ZeroMmap) Truncate(size int64) error {
	// err := zm.f.Truncate(size)
	// if err != nil {
	// 	return err
	// }
	zm.fSize = size
	if zm.offset > size {
		zm.offset = size
	}
	return nil
}

func (zm *ZeroMmap) Seek(offset int64, whence int) (ret int64, err error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += zm.offset
	case io.SeekEnd:
		offset += zm.fSize
	}
	if offset < 0 {
		return 0, errors.New(strSeekOffInvaild)
	}
	zm.offset = offset
	return zm.offset, nil
}

func (zm *ZeroMmap) Read(n int) ([]byte, error) {
	data, err := zm.readAt(zm.offset, n)
	if err == nil {
		zm.offset += int64(len(data))
	}
	return data, err
}

func (zm *ZeroMmap) ReadAt(offset int64, n int) ([]byte, error) {
	return zm.readAt(offset, n)
}

//concurrent operations are unsafe
func (zm *ZeroMmap) Next(n int) ([]byte, error) {
	if n <= 0 {
		return nil, errors.New(strInvaildArg)
	}
	data, err := zm.nextAt(zm.offset, n)
	zm.offset += int64(len(data))
	return data, err
}

func (zm *ZeroMmap) NextAt(offset int64, n int) ([]byte, error) {
	if offset < 0 {
		return nil, errors.New(strNegativeOffset)
	}
	if n <= 0 {
		return nil, errors.New(strInvaildArg)
	}
	return zm.nextAt(offset, n)
}

func (zm *ZeroMmap) readCheckArgs(offset int64, n int) (int, error) {
	if offset < 0 {
		return 0, errors.New(strNegativeOffset)
	}
	if offset >= zm.fSize {
		return 0, io.EOF
	}
	if n <= 0 {
		return 0, errors.New(strInvaildArg)
	}
	readAll := int(zm.fSize - offset)
	if n > readAll {
		n = readAll
	}
	return n, nil
}

func (zm *ZeroMmap) readAt(offset int64, n int) (data []byte, err error) {
	n, err = zm.readCheckArgs(offset, n)
	if err != nil {
		return nil, err
	}
	for {
		data, err = zm.allocator.AllocAt(offset, n)
		if err == allocate.End || len(data) < n {
			size := zm.mmSize
			if n > size {
				size = n
			}
			err = zm.allocator.Resize(offset, size)
			if err != nil {
				return nil, syscall.EAGAIN
			}
			zm.mmOff = offset
			continue
		}
		return
	}
}

//nextAt 会对文件进行扩展
func (zm *ZeroMmap) nextAt(offset int64, n int) ([]byte, error) {
	var (
		nextEnd = offset + int64(n)
	)
	if nextEnd > zm.fSize {
		if err := zm.f.Truncate(nextEnd); err != nil {
			return nil, err
		}
		zm.fSize = nextEnd
	}
	for {
		data, err := zm.allocator.AllocAt(offset, n)
		if err == allocate.End || len(data) < n {
			size := zm.mmSize
			if n > size {
				size = n
			}
			err = zm.allocator.Resize(offset, size)
			if err != nil {
				return nil, syscall.EAGAIN
			}
			zm.mmOff = offset
			continue
		}
		zm.waitSync = mergeArea(zm.waitSync, area{
			off: offset,
			len: len(data),
		})
		return data, err
	}
}

func (zm *ZeroMmap) WriteBack() error {
	return nil
}

func (zm *ZeroMmap) Sync() error {
	overlap := overlapArea(zm.waitSync, area{
		off: zm.mmOff,
		len: zm.allocator.Size(),
	})
	if overlap.len == 0 {
		return nil
	}
	buf, err := zm.allocator.AllocAt(overlap.off, overlap.len)
	if err != nil {
		if err == allocate.End {
			return nil
		}
		return err
	}

	if err := unix.Msync(buf, unix.MS_SYNC); err != nil {
		return err
	}
	zm.waitSync = area{
		off: overlap.off + int64(overlap.len),
	}
	return nil
}

func (zm *ZeroMmap) Close() error {
	if zm.allocator != nil {
		zm.allocator.Release()
		zm.allocator = nil
	}
	// if zm.f != nil {
	// 	if err := zm.f.Close(); err != nil {
	// 		return err
	// 	}
	// 	zm.f = nil
	// }
	return nil
}

func (zm *ZeroMmap) Size() int64 {
	info, err := zm.f.Stat()
	if err != nil {
		return -1
	}
	return info.Size()
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

func mergeArea(a area, b area) area {
	if a.len == 0 {
		return b
	}
	if b.len == 0 {
		return a
	}
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
