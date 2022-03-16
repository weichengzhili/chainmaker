/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package allocate

import (
	"errors"
	"os"
	"syscall"
)

var (
	OsPageSize        = os.Getpagesize()
	strNegativeOffset = "negative offset"
	End               = errors.New("END")
)

type MmapAllocator struct {
	mmapInfo
	f        *os.File //映射的文件
	mmapPort int
	mmapFlag int
	mmapLock bool
}

type mmapInfo struct {
	mmOff  int64  //映射偏移
	mmSize int    //映射大小
	mmArea []byte //映射的区域
}

func NewMmapAllocator(f *os.File, offset int64, mmSize int, mapPort, mapFlag int, lock bool) (*MmapAllocator, error) {
	allocator := &MmapAllocator{
		f: f,
		mmapInfo: mmapInfo{
			mmOff:  offset,
			mmSize: mmSize,
		},
		mmapPort: mapPort,
		mmapFlag: mapFlag,
		mmapLock: lock,
	}
	if err := allocator.remap(offset, mmSize); err != nil {
		return nil, err
	}
	return allocator, nil
}

func mmap(fd int, offset int64, size int, port int, flag int, lock bool) (_ mmapInfo, err error) {
	// size = int(alignUp(uint64(size), uint64(OsPageSize)))
	end := offset + int64(size)
	offset = int64(alignDown(uint64(offset), uint64(OsPageSize)))
	size = int(alignUp(uint64(end-offset), uint64(OsPageSize))) //避免分配的内存缩水
	var (
		area []byte
	)
	area, err = syscall.Mmap(fd, offset, size, port, flag)
	if err != nil {
		return
	}
	if lock {
		err = syscall.Mlock(area)
		if err != nil {
			return
		}
	}
	return mmapInfo{
		mmArea: area,
		mmSize: size,
		mmOff:  offset,
	}, nil
}

func (mal *MmapAllocator) remap(offset int64, size int) error {
	var (
		err error
		fd  int = -1
	)
	if mal.f != nil {
		fd = int(mal.f.Fd())
	} else {
		offset = 0
	}

	mmi, err := mmap(fd, offset, size, mal.mmapPort, mal.mmapFlag, mal.mmapLock)
	if err != nil {
		return err
	}

	if mal.mmapLock {
		syscall.Mlock(mmi.mmArea)
	}

	if mal.mmArea != nil {
		syscall.Munmap(mal.mmArea)
		// err = syscall.Munmap(mal.mmArea)
		// if err != nil {
		// 	return err
		// }
	}
	mal.mmArea = mmi.mmArea
	mal.mmOff = mmi.mmOff
	mal.mmSize = mmi.mmSize
	return nil
}

func (mal *MmapAllocator) AllocAt(offset int64, n int) ([]byte, error) {
	if offset < 0 {
		return nil, errors.New(strNegativeOffset)
	}
	return mal.allocAt(offset, n)
}

func (mal *MmapAllocator) allocAt(offset int64, n int) ([]byte, error) {
	var (
		mmEnd = mal.mmOff + int64(mal.mmSize)
	)
	if offset >= mmEnd || offset < mal.mmOff {
		return nil, End
	}

	if mmEnd-offset < int64(n) {
		n = int(mmEnd - offset)
	}
	from := offset - mal.mmOff
	return mal.mmArea[from : from+int64(n)], nil
}

func (mal *MmapAllocator) Size() int {
	return len(mal.mmArea)
}

func (mal *MmapAllocator) Release() {
	if mal.mmArea != nil {
		syscall.Munmap(mal.mmArea)
		mal.mmArea = nil
	}
}

func (mal *MmapAllocator) Resize(foffset int64, mmSize int) error {
	return mal.remap(foffset, mmSize)
}

func alignUp(n, a uint64) uint64 {
	return (n + a - 1) &^ (a - 1)
}

func alignDown(n, a uint64) uint64 {
	return n &^ (a - 1)
}
