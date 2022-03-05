/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package lws

import (
	"errors"
	"io"
)

var (
	errSeek           = errors.New("buffer seek out of range")
	errResizeArgument = errors.New("resize argument invalid")
	minGrowLen        = 1024
)

type buffer struct {
	buf  []byte
	woff int //写入文件的偏移
	off  int
}

func NewBuffer(cap int) *buffer {
	return &buffer{
		buf: make([]byte, 0, cap),
	}
}

func (b *buffer) FillFrom(r io.Reader) error {
	for {
		if len(b.buf) == cap(b.buf) {
			b.buf = append(b.buf, 0)[:len(b.buf)]
		}
		n, err := r.Read(b.buf[len(b.buf):cap(b.buf)])
		b.buf = b.buf[:len(b.buf)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			b.off = len(b.buf)
			b.woff = b.off
			return err
		}
	}
}

func (b *buffer) WriteByte(c byte) error {
	if n := b.shouldGrow(1); n > 0 {
		b.grow(n)
	}
	if b.off < len(b.buf) {
		b.buf[b.off] = c
	} else {
		b.buf = append(b.buf, c)
	}
	b.off++
	return nil
}

func (b *buffer) Write(data []byte) error {
	l := len(data)
	if n := b.shouldGrow(l); n > 0 {
		b.grow(n)
	}
	b.off += copy(b.buf[b.off:], data)
	return nil
}

func (b *buffer) Peek(n int) []byte {
	s := b.Size()
	if s < n {
		n = s
	}
	return b.buf[b.off : b.off+n]
}

func (b *buffer) ReadAt(pos, n int) []byte {
	l := len(b.buf)
	if pos < 0 || pos > l || n < 0 {
		return nil
	}
	if pos+n > l {
		n = l - pos
	}
	return b.buf[pos : pos+n]
}

func (b *buffer) Next(n int) []byte {
	data := b.Peek(n)
	b.off += len(data)
	return data
}

//基于当前位置进行偏移设置
func (b *buffer) Seek(n int) (int, error) {
	if n < -b.off || n+b.off > len(b.buf) {
		return 0, errSeek
	}
	b.off += n
	if b.woff > b.off {
		b.woff = b.off
	}
	return b.off, nil
}

func (b *buffer) Resize(s int) error {
	if s < 0 {
		return errResizeArgument
	}
	diff := s - b.Size()
	if n := b.shouldGrow(diff); n > 0 {
		b.grow(n)
	}
	b.buf = b.buf[:s]
	return nil
}

func (b *buffer) Truncate(n int) error {
	if n < 0 || n > b.Size() {
		return ErrTruncate
	}
	b.buf = b.buf[:n]
	if n < b.off {
		b.off = n
	}
	if b.woff > n {
		b.woff = n
	}
	return nil
}

func (b *buffer) Reset() {
	b.buf = b.buf[:0]
	b.off = 0
}

func (b *buffer) WriteTo(wr io.Writer) (int, error) {
	if b.woff == b.off {
		return 0, nil
	}
	n, err := wr.Write(b.buf[b.woff:b.off])
	b.woff += n
	return n, err
}

func (b *buffer) shouldGrow(n int) int {
	return n - b.Remain()
}

func (b *buffer) grow(n int) {
	if n < minGrowLen {
		n = minGrowLen
	}
	buf := make([]byte, 0, cap(b.buf)+n)
	copy(buf, b.buf)
	b.buf = buf
}

func (b *buffer) Remain() int {
	return cap(b.buf) - b.off + 1
}

func (b *buffer) Size() int {
	return len(b.buf)
}
