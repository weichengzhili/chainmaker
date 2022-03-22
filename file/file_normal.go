/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/

package file

import (
	"os"
)

type NormalFile struct {
	*os.File
}

func NewFile(path string) (*NormalFile, error) {
	return OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
}

func OpenFile(path string, flag int, perm os.FileMode) (*NormalFile, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
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
