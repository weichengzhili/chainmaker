/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package lws

type EntryContainer interface {
	FirstIndex() uint64
	LastIndex() uint64
	GetLogEntry(idx uint64) (*LogEntry, error)
	GetCoder(int8) (Coder, error)
	ReaderRelease()
}

type walContainer struct {
	wal   *Lws
	first uint64 //first 第一个log entry的位置
	last  uint64 //最新log entry的位置
}

func (wc *walContainer) FirstIndex() uint64 {
	return wc.first
}

func (wc *walContainer) LastIndex() uint64 {
	return wc.last
}

func (wc *walContainer) GetLogEntry(idx uint64) (*LogEntry, error) {
	sr, err := wc.wal.findReaderByIndex(idx)
	if err != nil {
		return nil, err
	}
	sr.Obtain()
	defer sr.Release()
	return sr.ReadLogByIndex(idx)
}
func (wc *walContainer) GetCoder(t int8) (Coder, error) {
	return wc.wal.coders.GetCoder(t)
}

func (wc *walContainer) ReaderRelease() {
	wc.wal.readRelease()
}

type fileContainer struct {
	*SegmentReader
	coders *coderMap
}

func (fc *fileContainer) GetLogEntry(idx uint64) (*LogEntry, error) {
	return fc.ReadLogByIndex(idx)
}
func (fc *fileContainer) GetCoder(t int8) (Coder, error) {
	return fc.coders.GetCoder(t)
}

func (fc *fileContainer) ReaderRelease() {
}

type EntryIterator struct {
	index     uint64 //迭代器当前的位置
	free      bool
	container EntryContainer //日志容器
}

type EntryElemnet struct {
	index     uint64
	container EntryContainer
	data      *LogEntry
	err       error
}

func newEntryIterator(c EntryContainer) *EntryIterator {
	return &EntryIterator{
		container: c,
		index:     c.FirstIndex() - 1,
	}
}

func (it *EntryIterator) SkipToFirst() {
	it.index = it.container.FirstIndex() - 1
}

func (it *EntryIterator) SkipToLast() {
	it.index = it.container.LastIndex() + 1
}

func (it *EntryIterator) HasNext() bool {
	return it.HasNextN(1)
}

func (it *EntryIterator) HasNextN(n int) bool {
	return int(it.container.LastIndex()-it.index) >= n
}

func (it *EntryIterator) Next() *EntryElemnet {
	return it.NextN(1)
}

func (it *EntryIterator) NextN(n int) *EntryElemnet {
	it.index += uint64(n)
	return it.element()
}

func (it *EntryIterator) PreviousN(n int) *EntryElemnet {
	it.index -= uint64(n)
	return it.element()
}

func (it *EntryIterator) element() *EntryElemnet {
	return &EntryElemnet{
		index:     it.index,
		container: it.container,
	}
}

func (it *EntryIterator) HasPreN(n int) bool {
	return int(it.index-it.container.FirstIndex()) >= n
}

func (it *EntryIterator) HasPre() bool {
	return it.HasPreN(1)
}

func (it *EntryIterator) Previous() *EntryElemnet {
	return it.PreviousN(1)
}

func (it *EntryIterator) Release() {
	if it.free {
		return
	}
	it.free = true
	it.container.ReaderRelease()
}

func (ele *EntryElemnet) get() (*LogEntry, error) {
	if ele.err != nil {
		return nil, ele.err
	}
	if ele.data != nil {
		return ele.data, nil
	}
	ele.data, ele.err = ele.container.GetLogEntry(ele.index)
	return ele.data, ele.err
}

func (ele *EntryElemnet) Index() uint64 {
	return ele.index
}

func (ele *EntryElemnet) Get() ([]byte, error) {
	entry, err := ele.get()
	if err != nil {
		return nil, err
	}
	return entry.Data, nil
}

func (ele *EntryElemnet) GetObj() (interface{}, error) {
	entry, err := ele.get()
	if err != nil {
		return nil, err
	}
	if entry.Typ == RawCoderType {
		return entry.Data, nil
	}
	coder, err := ele.container.GetCoder(entry.Typ)
	if err != nil {
		return nil, err
	}
	return coder.Decode(entry.Data)
}
