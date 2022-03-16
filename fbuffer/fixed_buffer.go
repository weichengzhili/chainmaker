/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package fbuffer

import (
	"errors"
	"io"
	"math"
	"os"

	"chainmaker.org/chainmaker/lws/allocate"
)

type fixedbuffer struct {
	mmOff     int64 //buffer对应的文件的偏移量
	offset    int64
	fSize     int64
	allocator *allocate.BytesAllocator
	f         *os.File
	waitSync  area
	initSize  int
}

func NewFixedBuffer(f *os.File, bufSize int) (*fixedbuffer, error) {
	finfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return &fixedbuffer{
		allocator: allocate.NewBytesAllocator(0),
		f:         f,
		fSize:     finfo.Size(),
		initSize:  bufSize,
	}, nil
}

func (b *fixedbuffer) Truncate(n int64) error {
	if n < 0 {
		return errors.New(strInvaildArg)
	}
	if b.offset > n {
		b.offset = n
	}
	b.fSize = n
	return nil
}

func (b *fixedbuffer) Seek(offset int64, whence int) (int64, error) {
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

func (b *fixedbuffer) Read(n int) ([]byte, error) {
	data, err := b.readAt(b.offset, n)
	if err == nil {
		b.offset += int64(len(data))
	}
	return data, err
}

func (b *fixedbuffer) ReadAt(offset int64, n int) ([]byte, error) {
	return b.readAt(offset, n)
}

func (fb *fixedbuffer) readCheckArgs(offset int64, n int) (int, error) {
	if offset < 0 {
		return 0, errors.New(strNegativeOffset)
	}
	if offset >= fb.fSize {
		return 0, io.EOF
	}
	if n <= 0 {
		return 0, errors.New(strInvaildArg)
	}
	readAll := int(fb.fSize - offset)
	if n > readAll {
		n = readAll
	}
	return n, nil
}

func (b *fixedbuffer) readAt(offset int64, n int) ([]byte, error) {
	n, err := b.readCheckArgs(offset, n)
	if err != nil {
		return nil, err
	}

	for {
		buf, err := b.allocator.AllocAt(offset-b.mmOff, n)
		if err == allocate.End || len(buf) < n {
			if err = b.rebuffer(offset, n, true); err != nil {
				return nil, err
			}
			continue
		}

		return buf, err
	}
}

func (b *fixedbuffer) rebuffer(offset int64, n int, fill bool) error {
	if err := b.writeFile(); err != nil {
		return err
	}
	size := b.allocator.Size()
	if size == 0 {
		size = b.initSize
	}
	if size < n {
		size = n
	}
	if size > b.allocator.Size() {
		b.allocator.Resize(size)
	}

	b.mmOff = offset
	readN := 0
	if fill {
		all, _ := b.allocator.AllocAt(0, size)
		for {
			rn, err := b.f.ReadAt(all, offset)
			readN += rn
			if err != nil {
				if err == io.EOF {
					err = nil
				} else {
					b.mmOff = math.MaxInt64
				}
				return err
			}
			if readN == size {
				break
			}
		}
	}

	return nil
}

func (b *fixedbuffer) Next(n int) ([]byte, error) {
	if n <= 0 {
		return nil, errors.New(strInvaildArg)
	}
	buf, err := b.nextAt(b.offset, n)
	if err != nil {
		return nil, err
	}
	b.offset += int64(len(buf))
	return buf, nil
}

func (b *fixedbuffer) NextAt(offset int64, n int) ([]byte, error) {
	if offset < 0 {
		return nil, errors.New(strNegativeOffset)
	}
	if n <= 0 {
		return nil, errors.New(strInvaildArg)
	}
	return b.nextAt(offset, n)
}

func (b *fixedbuffer) nextAt(offset int64, n int) ([]byte, error) {
	for {
		buf, err := b.allocator.AllocAt(offset-b.mmOff, n)
		if err == allocate.End || len(buf) < n {
			if err = b.rebuffer(offset, n, true); err != nil {
				return nil, err
			}
			continue
		}
		b.waitSync = mergeArea(b.waitSync, area{
			off: offset,
			len: n,
		})
		if offset+int64(n) > b.fSize {
			b.fSize = offset + int64(n)
		}
		return buf, err
	}
}

func (b *fixedbuffer) writeFile() error {
	if b.waitSync.len == 0 {
		return nil
	}
	buf, err := b.allocator.AllocAt(b.waitSync.off-b.mmOff, b.waitSync.len)
	if err != nil {
		return err
	}
	_, err = b.f.WriteAt(buf, b.waitSync.off)
	if err == nil {
		b.waitSync = area{}
	}
	return err
}

func (b *fixedbuffer) WriteBack() error {
	return b.writeFile()
}

func (b *fixedbuffer) Size() int64 {
	return b.fSize
}

func (b *fixedbuffer) Close() error {
	if err := b.writeFile(); err != nil {
		return err
	}
	if b.allocator != nil {
		b.allocator.Release()
		b.allocator = nil
	}
	return nil
}
