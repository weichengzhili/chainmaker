/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package allocate

import (
	"errors"
)

type BytesAllocator struct {
	buf []byte
}

func NewBytesAllocator(cap int) *BytesAllocator {
	return &BytesAllocator{
		buf: make([]byte, cap, cap),
	}
}

//AllocAt 在分配器中获取[offset:offset+n)范围内的缓存区, 如果offset不在分配器范围则返回END错误，如果offset+n超过分配器上限，则返回的缓存区大小将会小于n
func (bsa *BytesAllocator) AllocAt(offset int64, n int) ([]byte, error) {
	if offset < 0 {
		return nil, errors.New(strNegativeOffset)
	}
	return bsa.allocAt(offset, n)
}

func (bsa *BytesAllocator) allocAt(offset int64, n int) ([]byte, error) {
	var (
		end = int64(bsa.Size())
	)
	if offset >= end || offset < 0 {
		return nil, End
	}

	if end-offset < int64(n) {
		n = int(end - offset)
	}
	return bsa.buf[offset : offset+int64(n)], nil
}

func (bsa *BytesAllocator) Size() int {
	return len(bsa.buf)
}

func (bsa *BytesAllocator) Release() {
	if bsa.buf != nil {
		bsa.buf = nil
	}
}

func (bsa *BytesAllocator) Resize(size int) error {
	if size < bsa.Size() {
		return nil
	}
	buf := make([]byte, size, size)
	// copy(buf, bsa.buf)
	bsa.buf = buf
	return nil
}
