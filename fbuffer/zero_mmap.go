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
	waitSync  area     //待刷盘的区域
	mmSize    int      //映射区大小
	mmOff     int64    //映射区偏移量
	allocator *allocate.MmapAllocator
}
type area struct {
	off int64
	len int
}

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

//Truncate 同步文件的大小，一般对文件进行Truncate的时候，同步调用buffer.Truncate,fSize防止从映射区中读取的数据超出文件大小
func (zm *ZeroMmap) Truncate(size int64) error {
	if size < 0 {
		return errors.New(strInvaildArg)
	}
	zm.fSize = size
	return nil
}

//ReadAt 从offset处读取n个字节，除非读到文件末尾，否则读取到的长度一定为n, 其主要用来读取文件的内容
func (zm *ZeroMmap) ReadAt(offset int64, n int) ([]byte, error) {
	return zm.readAt(offset, n)
}

//NextAt 从offset处获取n个字节，如果参数合法，则获取到bytes长度一定为n, offset可以比当前文件的size大，获取的bytes用于写入数据
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
			zm.waitSync = area{}
			continue
		}
		return
	}
}

//nextAt 如果获取的数据超过了文件的长度，则先Truncate文件，以防止映射区写入出错，然后再重新映射；获取到缓存后，会将缓存对应的区域合并到waitSync， 以在上层调用刷盘的时候，将waitSync标记的区域内的数据刷新到磁盘
//因为在每次重映射的时候会系统会主动将映射区数据同步到底层文件，故waitSync会在重映射时重置
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
			zm.waitSync = area{}
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

//Sync 将映射区的数据刷新到磁盘
func (zm *ZeroMmap) Sync() error {
	//为安全期间，获取waitSync和映射区的交集范围
	overlap := overlapArea(zm.waitSync, area{
		off: zm.mmOff,
		len: zm.allocator.Size(),
	})
	if overlap.len == 0 {
		return nil
	}
	//将交集的offset进行页对齐，防止sync失败
	off := int64(alignDown(uint64(overlap.off), uint64(OsPageSize)))
	overlap = area{
		off: off,
		len: int(overlap.off-off) + overlap.len,
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
	zm.waitSync = area{}
	return nil
}

//Close 释放分配器
func (zm *ZeroMmap) Close() error {
	if zm.allocator != nil {
		zm.allocator.Release()
		zm.allocator = nil
	}
	return nil
}

func (zm *ZeroMmap) Size() int64 {
	info, err := zm.f.Stat()
	if err != nil {
		return -1
	}
	return info.Size()
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
