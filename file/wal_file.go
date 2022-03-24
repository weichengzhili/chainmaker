/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package file

var (
	strSeekOffInvaild = "seek offset invaild"
)

type WalFile interface {
	Write([]byte) (int, error)
	Read([]byte) (int, error)
	WriteAt(data []byte, offset int64) (int, error)
	ReadAt(data []byte, offset int64) (int, error)
	Size() int64
	Close() error
	Seek(offset int64, whence int) (ret int64, err error)
	Truncate(size int64) error
	Sync() error
}
