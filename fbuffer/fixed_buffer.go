/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package fbuffer

import (
	"errors"
	"io"
	"math"

	"chainmaker.org/chainmaker/lws/allocate"
)

type file interface {
	WriteAt([]byte, int64) (int, error)
	ReadAt([]byte, int64) (int, error)
	Size() int64
	Close() error
}

//fixedbuffer 固定大小的缓存
type fixedbuffer struct {
	mmOff     int64                    //buffer对应的文件的偏移量
	fSize     int64                    //文件的大小
	allocator *allocate.BytesAllocator //字节分配器
	f         file
	waitSync  area //等待同步的区域大小
	initSize  int  //缓存初始化大小
}

func NewFixedBuffer(f file, bufSize int) (*fixedbuffer, error) {
	return &fixedbuffer{
		allocator: allocate.NewBytesAllocator(0),
		f:         f,
		fSize:     f.Size(),
		initSize:  bufSize,
	}, nil
}

//Truncate 同步文件的大小，一般对文件进行Truncate的时候，同步调用buffer.Truncate,fSize防止从缓存中读取的数据超出文件大小
func (b *fixedbuffer) Truncate(n int64) error {
	if n < 0 {
		return errors.New(strInvaildArg)
	}
	b.fSize = n
	return nil
}

//ReadAt 从offset处读取n个字节，除非读到文件末尾，否则读取到的长度一定为n, 其主要用来读取文件的内容
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
		//如果超出分配器的范围或者分配的空间不足，则重新置换缓存，并将底层文件的内容映射到缓存中
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

//rebuffer 重新置换缓存，如果读取的数据比缓存大，则扩展缓存，否则原大小置换
func (b *fixedbuffer) rebuffer(offset int64, n int, fill bool) error {
	//如果缓存有脏数据，则需要将其会写到文件中
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

//NextAt 从offset处获取n个字节，如果参数合法，则获取到bytes长度一定为n, offset可以比当前文件的size大，获取的bytes用于写入数据
func (b *fixedbuffer) NextAt(offset int64, n int) ([]byte, error) {
	if offset < 0 {
		return nil, errors.New(strNegativeOffset)
	}
	if n <= 0 {
		return nil, errors.New(strInvaildArg)
	}
	return b.nextAt(offset, n)
}

//nextAt 从offset处获取n个字节,用于上层写入数据，并将获取的字节范围合并到waitSync中，以在置换缓存或者上层调用回写的时候，将waitSync标记的区域内的数据同步到文件中
//其还会增大fSize的值，以表示文件写入数据增大
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

//writeFile 将缓存中的数据同步到文件中
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

//WriteBack 将缓存中脏数据回写到文件中
func (b *fixedbuffer) WriteBack() error {
	return b.writeFile()
}

func (b *fixedbuffer) Size() int64 {
	return b.fSize
}

//Close 先将缓存会写到文件，然后释放缓存
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
