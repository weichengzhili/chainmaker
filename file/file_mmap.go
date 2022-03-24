/*
Copyright (C) BABEC. All rights reserved.
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

//MmapFile 内存映射式文件操作，不支持并发操作
type MmapFile struct {
	f      *os.File //映射的文件
	buf    *fbuffer.ZeroMmap
	offset int64
}

func NewMmapFile(path string, mmSize int) (*MmapFile, error) {
	return OpenMmapFile(path, mmSize, os.O_RDWR|os.O_CREATE, 0644, syscall.MAP_SHARED, false)
}

//OpenMmapFile 打开一个mmapfile path：文件路径，mmSize:映射区大小，flag：文件指定的flag， perm：创建文件的权限， mapFlag:映射时指定的flag， lock：是否锁定映射区
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

//fileFlagToMapPort 文件的读写权限转换到映射的读写权限
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

//Truncate 截断文件 同普通文件Truncate
func (mf *MmapFile) Truncate(size int64) error {
	if err := mf.f.Truncate(size); err != nil {
		return err
	}
	if mf.offset > size {
		mf.offset = size
	}

	return mf.buf.Truncate(size)
}

//Seek 设置读写偏移量 同普通文件Seek
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

//WriteAt 从offset处写入data数据，同普通文件WriteAt
func (mf *MmapFile) WriteAt(data []byte, offset int64) (int, error) {
	b, err := mf.buf.NextAt(offset, len(data))
	if err != nil {
		return 0, err
	}
	return copy(b, data), nil
}

//Write 追加写入数据 同普通文件Write
func (mf *MmapFile) Write(data []byte) (int, error) {
	b, err := mf.buf.NextAt(mf.offset, len(data))
	if err != nil {
		return 0, err
	}
	mf.offset += int64(len(b))
	return copy(b, data), nil
}

//Sync 将数据刷新到磁盘 同普通文件Sync
func (mf *MmapFile) Sync() error {
	return mf.buf.Sync()
}

//ReadAt 从offset处读取数据到data，同普通文件ReadAt
func (mf *MmapFile) ReadAt(data []byte, offset int64) (int, error) {
	b, err := mf.buf.ReadAt(offset, len(data))
	if err != nil {
		return 0, err
	}
	return copy(data, b), nil
}

//Read 顺序读取数据到data，同普通文件Read
func (mf *MmapFile) Read(data []byte) (int, error) {
	b, err := mf.buf.ReadAt(mf.offset, len(data))
	if err != nil {
		return 0, err
	}
	mf.offset += int64(len(b))
	return copy(data, b), nil
}

//Close 关闭映射区并关闭底层文件
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

//Size 获取文件的大小，因为使用的是mmap，故写入到映射区相当于写入到了os缓存区，此时调用的获取底层文件的size等同于操作普通文件一样
func (mf *MmapFile) Size() int64 {
	info, err := mf.f.Stat()
	if err != nil {
		return -1
	}
	return info.Size()
}
