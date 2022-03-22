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
	mmapInfo          //当前映射区信息
	f        *os.File //映射的文件
	mmapPort int      //映射所需的内存保护，如内存可读可写可执行权限
	mmapFlag int      //映射标识，如内存是否共享，修改是否更新到底层文件
	mmapLock bool     //映射是否锁定内存
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

//mmap 会对fd指向的文件进行内存映射处理，内部进行内存对齐计算，偏移量对系统页向下对齐， size在保持不缩减的情况下，对系统页向上对齐
func mmap(fd int, offset int64, size int, port int, flag int) (_ mmapInfo, err error) {
	end := offset + int64(size)
	offset = int64(alignDown(uint64(offset), uint64(OsPageSize)))
	size = int(alignUp(uint64(end-offset), uint64(OsPageSize))) //避免分配的内存缩水，如offset向下对齐，此时offset+size可能会变小
	var (
		area []byte
	)
	area, err = syscall.Mmap(fd, offset, size, port, flag)
	if err != nil {
		return
	}
	return mmapInfo{
		mmArea: area,
		mmSize: size,
		mmOff:  offset,
	}, nil
}

//remap 映射区置换，根据offset&size创建新的映射区，并释放旧的映射区
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

	mmi, err := mmap(fd, offset, size, mal.mmapPort, mal.mmapFlag)
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

//AllocAt 在映射区中获取[offset:offset+n)范围内的缓存区, 如果offset不在映射区范围则返回END错误，如果offset+n超过映射区上线，则返回的缓存区大小将会小于n
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

//Size 当前映射区的大小
func (mal *MmapAllocator) Size() int {
	return len(mal.mmArea)
}

//Release 释放映射区
func (mal *MmapAllocator) Release() {
	if mal.mmArea != nil {
		syscall.Munmap(mal.mmArea)
		mal.mmArea = nil
	}
}

//Resize 重映射映射区
func (mal *MmapAllocator) Resize(foffset int64, mmSize int) error {
	return mal.remap(foffset, mmSize)
}

func alignUp(n, a uint64) uint64 {
	return (n + a - 1) &^ (a - 1)
}

func alignDown(n, a uint64) uint64 {
	return n &^ (a - 1)
}
