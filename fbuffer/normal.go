/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package fbuffer

import (
	"errors"
	"io"

	"chainmaker.org/chainmaker/lws/allocate"
)

var (
	minGrowLen = 1024
)

type buffer struct {
	woff      int64 //写入文件的偏移
	offset    int64
	size      int64
	allocator *allocate.BytesAllocator
}

func NewBuffer(cap int) *buffer {
	return &buffer{
		allocator: allocate.NewBytesAllocator(cap),
	}
}

func (b *buffer) FillFrom(r io.Reader) error {
	readN := 0
	size := b.allocator.Size()
	for {
		buf, err := b.allocAt(b.offset, size)
		if err != allocate.End {
			b.allocator.Resize(2 * b.allocator.Size())
			continue
		}
		n, err := r.Read(buf)
		b.offset += int64(n)
		readN += n
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			b.woff = b.offset
			return err
		}
	}
}

func (b *buffer) allocAt(offset int64, n int) ([]byte, error) {
	for {
		buf, err := b.allocator.AllocAt(offset, n)
		if err != nil {
			if err == allocate.End || len(buf) < n {
				grow := minGrowLen
				if grow < n-len(buf) {
					grow = n - len(buf)
				}
				b.allocator.Resize(b.allocator.Size() + grow)
				continue
			}
			return nil, err
		}
		if offset+int64(n) > b.size {
			b.size = offset + int64(n)
		}
		return buf, nil
	}
}

func (b *buffer) WriteByte(c byte) error {
	buf, err := b.allocAt(b.offset, 1)
	if err != nil {
		return err
	}
	buf[0] = c
	b.offset++
	return nil
}

func (b *buffer) Write(data []byte) error {
	buf, err := b.allocAt(b.offset, len(data))
	if err != nil {
		return err
	}
	b.offset += int64(copy(buf, data))
	return nil
}

func (b *buffer) ReadAt(pos int64, n int) ([]byte, error) {
	l := b.Size()
	if pos < 0 || pos > int64(l) {
		return nil, io.EOF
	}

	buf, err := b.allocAt(pos, n)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (b *buffer) Next(n int) ([]byte, error) {
	buf, err := b.nextAt(b.offset, n)
	if err != nil {
		return nil, err
	}
	b.offset += int64(len(buf))
	return buf, nil
}

// func (b *buffer) NextAt(offset int64, n int) ([]byte, error) {

// }

func (b *buffer) nextAt(offset int64, n int) ([]byte, error) {
	buf, err := b.allocAt(offset, n)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (b *buffer) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += b.offset
	case io.SeekEnd:
		offset += b.offset
	}
	if offset < 0 {
		return 0, errors.New("strSeekOffInvaild")
	}
	b.offset = offset
	return b.offset, nil
}

func (b *buffer) Truncate(n int64) error {
	if n < 0 {
		return errors.New("")
	}
	if b.offset > n {
		b.offset = n
	}
	if b.woff > n {
		b.woff = n
	}
	return nil
}

func (b *buffer) WriteTo(wr io.Writer) (int, error) {
	if b.woff == b.offset {
		return 0, nil
	}
	buf, err := b.allocAt(b.woff, int(b.offset-b.woff))
	if err != nil {
		return 0, err
	}
	n, err := wr.Write(buf)
	b.woff += int64(n)
	return n, err
}

func (b *buffer) Size() int64 {
	return b.size
}
