package file

import (
	"io"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

type mmap_file struct {
	f      *os.File //映射的文件
	fSize  int64
	offset int64 //映射偏移
	// woffset int64 //写偏移量
	// roffset int64 //读偏移量
	mmOff  int64
	mmSize int    //映射大小
	mmArea []byte //映射的区域
}

func NewMmapFile(path string, mmSize int, fileSize int64) (*mmap_file, error) {
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
	if fSize < fileSize {
		err = syscall.Ftruncate(int(f.Fd()), fileSize)
		if err != nil {
			return nil, err
		}
		fSize = fileSize
	}

	buffer, err := syscall.Mmap(int(f.Fd()), 0, mmSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return &mmap_file{
		f:      f,
		fSize:  fSize,
		mmSize: mmSize,
		mmArea: buffer,
	}, nil
}

func (mf *mmap_file) Seek(offset int64, whence int) (ret int64, err error) {
	switch whence {
	case os.SEEK_SET:
		mf.offset = offset
	case os.SEEK_CUR:
		mf.offset += offset
	case os.SEEK_END:
		mf.offset = mf.fSize + offset
	}

	mf.offset = offset
	if mf.offset < mf.mmOff || mf.offset > mf.mmOff+int64(mf.mmSize) {
		mf.remap(mf.offset)
	}
	return mf.offset, nil
}

func (mf *mmap_file) remap(offset int64) error {
	var err error
	if mf.mmArea != nil {
		err = syscall.Munmap(mf.mmArea)
		if err != nil {
			return err
		}
	}

	mf.mmArea, err = syscall.Mmap(int(mf.f.Fd()), offset, mf.mmSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	mf.mmOff = offset
	return err
}

func (mf *mmap_file) Write(data []byte) (int, error) {
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
		if mf.offset >= mmEnd || mf.mmArea == nil {
			if err := mf.remap(mf.offset); err != nil {
				return 0, syscall.EAGAIN
			}
			mmEnd = mf.mmOff + int64(mf.mmSize)
		}
		writeN += copy(mf.mmArea[mf.offset-mf.mmOff:], data[writeN:])
		mf.offset += int64(writeN)
		if mf.offset > mf.fSize {
			mf.fSize = mf.offset
		}
	}
	return writeN, nil
}

func (mf *mmap_file) Flush() error {
	if mf.mmArea != nil {
		return unix.Msync(mf.mmArea, unix.MS_SYNC)
	}
	return nil
}

func (mf *mmap_file) Read(data []byte) (int, error) {
	var (
		readN int
		mmEnd = mf.mmOff + int64(mf.mmSize)
	)

	// for (mf.offset >= mf.mmOff && mf.offset <= mf.fSize) || readN == cap(data) {
	for mf.offset < mf.fSize && readN < cap(data) {
		if mf.offset >= mmEnd {
			if err := mf.remap(mf.offset); err != nil {
				return readN, syscall.EAGAIN
			}
			mmEnd = mf.mmOff + int64(mf.mmSize)
		}
		readN += copy(data[readN:], mf.mmArea[mf.offset-mf.mmOff:])
		mf.offset += int64(readN)
	}
	if mf.offset == mf.fSize {
		return readN, io.EOF
	}
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

func (mf *mmap_file) Size() int64 {
	mf.f.Sync()
	info, err := mf.f.Stat()
	if err != nil {
		return -1
	}
	return info.Size()
}
