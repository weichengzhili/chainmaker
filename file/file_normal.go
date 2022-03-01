/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package file

import (
	"os"
	"syscall"
)

type NormalFile struct {
	*os.File
}

func NewFile(path string, fileSize int64) (*NormalFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	// defer func() {
	// 	if err != nil {
	// 		f.Close()
	// 	}
	// }()
	finfo, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if finfo.Size() < fileSize {
		err = syscall.Ftruncate(int(f.Fd()), fileSize)
		if err != nil {
			f.Close()
			return nil, err
		}
	}
	return &NormalFile{
		File: f,
	}, nil
}

func (fn *NormalFile) Size() int64 {
	info, err := fn.Stat()
	if err != nil {
		return -1
	}
	return info.Size()
}

func (fn *NormalFile) Flush() error {
	return fn.File.Sync()
}
