package file

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

type mmap_file struct {
	f       *os.File //映射的文件
	offset  int64    //映射偏移
	woffset int64    //写偏移量
	roffset int64    //读偏移量
	size    int      //映射大小
	mmArea  []byte   //映射的区域
}

func NewMmapFile(path string, mmSize int, fileSize int64) (*mmap_file, error) {
	f1, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			f1.Close()
		}
	}()
	finfo, err := f1.Stat()
	if err != nil {
		return nil, err
	}
	if finfo.Size() < fileSize {
		err = syscall.Ftruncate(int(f1.Fd()), fileSize)
		if err != nil {
			return nil, err
		}
	}

	buffer, err := syscall.Mmap(int(f1.Fd()), 0, mmSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return &mmap_file{
		f:      f1,
		size:   mmSize,
		mmArea: buffer,
	}, nil
}

func (mf *mmap_file) setOffset(offset int64) {
	mf.offset = offset
}

func (mf *mmap_file) remap(offset int64) error {
	var err error
	if mf.mmArea != nil {
		err = syscall.Munmap(mf.mmArea)
		if err != nil {
			return err
		}
	}

	mf.mmArea, err = syscall.Mmap(int(mf.f.Fd()), offset, mf.size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	return err
}

func (mf *mmap_file) Write(data []byte) (int, error) {
	if mf.mmArea == nil {
		err := mf.remap(mf.offset)
		if err != nil {
			return 0, err
		}
	}
	//如果写入的数据超越了映射区需要重新映射，再写入
	mf.woffset += int64(copy(mf.mmArea[mf.woffset:], data))
	return 0, nil
}

func (mf *mmap_file) Flush() error {
	if mf.mmArea != nil {
		return unix.Msync(mf.mmArea, unix.MS_SYNC)
	}
	return nil
}

func (mf *mmap_file) Read(data []byte) (int, error) {
	//这里需要有一层判断，如果写映射区包含读的区域，则直接通过映射区读取，否则的话直接从文件读取
	readN, err := mf.f.ReadAt(data, mf.roffset)
	if err != nil {
		return 0, err
	}
	mf.roffset += int64(readN)
	return readN, nil
}

func (mf *mmap_file) Close() error {
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
