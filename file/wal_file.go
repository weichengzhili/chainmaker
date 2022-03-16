/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/

package file

import "os"

var (
	OsPageSize        = os.Getpagesize()
	strNegativeOffset = "negative offset"
	strSeekOffInvaild = "seek offset invaild"
	strInvaildArg     = "arguments invaild"
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
