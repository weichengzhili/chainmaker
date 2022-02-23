package file

import (
	"os"
)

type file_normal struct {
	*os.File
}

func NewFile(path string, fileSize int64) (*file_normal, error) {
	f1, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	// finfo, err := f1.Stat()
	// finfo.Size()
	// defer func() {
	// 	if err != nil {
	// 		f1.Close()
	// 	}
	// }()
	// finfo, err := f1.Stat()
	// if err != nil {
	// 	return nil, err
	// }
	// if finfo.Size() < fileSize {
	// 	err = syscall.Ftruncate(int(f1.Fd()), fileSize)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }
	return &file_normal{
		File: f1,
	}, nil
}

func (fn *file_normal) Flush() error {
	return fn.File.Sync()
}
