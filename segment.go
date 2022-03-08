/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package lws

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"sync"
	"time"

	"chainmaker.org/chainmaker/lws/file"
)

const (
	file_mmap_size = 1 << 26
	checkSumPoly   = 0xD5828281
	bufferSize     = 1 << 26

	lenSize   = 4
	crc32Size = 4
	typeSize  = 1
	metaSize  = lenSize + crc32Size + typeSize
)

var (
	ErrFileTypeNotSupport = errors.New("this file type is not supported")
	ErrNoEnoughData       = errors.New("no enough data in buffer")
	ErrTruncate           = errors.New("truncation out of range")
	ErrSegmentIndex       = errors.New("index out of segment range")
)

type WriteFlusher interface {
	Write([]byte) (int, error)
	Flush() error
}

type posEntry struct {
	*LogEntry
	pos int
}

type LogEntry struct {
	Len   int //crc32 + typ + data总长度
	Crc32 uint32
	Typ   int8
	Data  []byte
}

type Segment struct {
	ID    uint64 //文件编号
	Size  uint64 //文件当前大小
	Index uint64 //文件中日志的最小索引
	Path  string //文件路径
}

type crc32er struct {
	poly  uint32
	table *crc32.Table
}

func (crc *crc32er) Checksum(data []byte) uint32 {
	return crc32.Checksum(data, crc.table)
}

func newCrc32er(poly uint32) *crc32er {
	return &crc32er{
		poly:  poly,
		table: crc32.MakeTable(poly),
	}
}

type SegmentWriter struct {
	*SegmentProcessor
	ft          FileType
	wf          WriteFlag //刷盘策略
	threshold   int
	acc         int //等待刷盘的累计值
	segmentSize uint64
	count       int //写入条目的数量
	closeCh     chan struct{}
	writeLocker sync.Mutex //非同步写情况下，可能会导致并发写相同数据
}

type WriterOptions struct {
	SegmentSize uint64
	Ft          FileType
	Wf          WriteFlag
	Fv          int
}

func NewSegmentWriter(s *Segment, opt WriterOptions) (*SegmentWriter, error) {
	sw := &SegmentWriter{
		SegmentProcessor: newSegmentProcessor(s),
		ft:               opt.Ft,
		wf:               opt.Wf,
		segmentSize:      opt.SegmentSize,
		threshold:        opt.Fv,
		closeCh:          make(chan struct{}),
	}
	if err := sw.open(); err != nil {
		return nil, err
	}
	if err := sw.readAndCheck(); err != nil {
		return nil, err
	}
	sw.startFlushWorker()
	return sw, nil
}

func (sw *SegmentWriter) open() error {
	return sw.SegmentProcessor.open(sw.ft, int64(sw.segmentSize))
}

func (sw *SegmentWriter) readAndCheck() (err error) {
	if err = sw.readToBuffer(); err != nil {
		return
	}

	sw.traverseLogEntries(func(ue *posEntry) bool {
		if ue.LogEntry == nil || ue.Len == 0 || !sw.crc32Check(ue.Crc32, ue.Data) {
			sw.Truncate(ue.pos)
			return true
		}
		sw.count++
		return false
	})
	return
}

func (sw *SegmentWriter) startFlushWorker() {
	if sw.wf&(^WF_SYNCWRITE) == WF_TIMEDFLUSH {
		go sw.flushTimeDelay()
	}
}

func (sw *SegmentWriter) flushTimeDelay() {
	if sw.threshold <= 0 {
		sw.threshold = timeDelay
	}
	t := time.Millisecond * time.Duration(sw.threshold)
	timer := time.NewTimer(t)
	for {
		select {
		case <-timer.C:
			if sw.acc == 0 {
				continue
			}
			sw.Flush()
			timer.Reset(t)
		case <-sw.closeCh:
			return
		}
	}
}

func (sw *SegmentWriter) EntryCount() int {
	return sw.count
}

func (sw *SegmentWriter) Replace(s *Segment) error {
	if sw.s.ID == s.ID {
		return nil
	}
	if err := sw.Flush(); err != nil {
		return err
	}
	old := sw.s
	sw.s = s
	if err := sw.open(); err != nil {
		sw.s = old
		return err
	}
	sw.count = 0
	sw.buf.Reset()
	return nil
}

func (sw *SegmentWriter) Write(t int8, data []byte) (int, error) {
	sw.writeLocker.Lock()
	l := sw.writeToBuffer(t, data)
	if sw.wf&WF_SYNCWRITE == WF_SYNCWRITE {
		if _, err := sw.buf.WriteTo(sw.f); err != nil {
			sw.buf.Seek(-l)
			sw.writeLocker.Unlock()
			return 0, err
		}
	}
	sw.writeLocker.Unlock()
	err := sw.tryFlush()
	// if err != nil {
	// 	return 0, nil
	// }
	return l, err
}

//直接写不经过缓存
func (sw *SegmentWriter) directWrite(t int8, data []byte) (int, error) {
	metaLen := lenSize + crc32Size + typeSize
	meta := make([]byte, metaLen)
	sw.serializateUint32(meta, uint32(crc32Size+typeSize+len(data)))
	sw.serializateUint32(meta[lenSize:], sw.crc32er.Checksum(data))
	meta[lenSize+crc32Size] = byte(t)
	n1, err := sw.f.Write(meta)
	if err != nil {
		return n1, err
	}
	n2, err := sw.f.Write(data)
	if err != nil {
		return n1 + n2, err
	}
	return n1 + n2, sw.f.Sync()
}

func (sw *SegmentWriter) writeToBuffer(t int8, data []byte) int {
	sw.count++
	return sw.writeLog(t, data)
}

func (sw *SegmentWriter) tryFlush() error {
	if sw.wf&WF_SYNCFLUSH == WF_SYNCFLUSH {
		return sw.Flush()
	}
	sw.acc++
	if sw.wf&WF_QUOTAFLUSH == WF_QUOTAFLUSH && sw.acc >= sw.threshold {
		return sw.Flush()
	}

	return nil
}

func (sw *SegmentWriter) Size() uint64 {
	return uint64(sw.buf.Size())
}

func (sw *SegmentWriter) Flush() error {
	if sw.wf&WF_SYNCWRITE != WF_SYNCWRITE {
		sw.writeLocker.Lock()
		_, err := sw.buf.WriteTo(sw.f)
		sw.writeLocker.Unlock()
		if err != nil {
			return err
		}
	}
	err := sw.f.Sync()
	if err == nil {
		sw.acc = 0
	}
	return err
}

func (sw *SegmentWriter) Close() error {
	close(sw.closeCh)
	return sw.SegmentProcessor.Close()
}

type SegmentReader struct {
	*SegmentProcessor
	pos []int //记录每个entry的起始位置
}

func NewSegmentReader(s *Segment, ft FileType) (*SegmentReader, error) {
	var (
		sr = &SegmentReader{
			SegmentProcessor: newSegmentProcessor(s),
		}
		err error
	)

	if err = sr.open(ft, 0); err != nil {
		return nil, err
	}

	if err = sr.loadEntries(); err != nil {
		sr.Close()
		return nil, err
	}
	return sr, nil
}

func (sr *SegmentReader) loadEntries() error {
	if err := sr.readToBuffer(); err != nil {
		return err
	}
	call := func(ue *posEntry) bool {
		if ue.LogEntry == nil || ue.Len == 0 || !sr.crc32Check(ue.Crc32, ue.Data) {
			return true
		}

		sr.pos = append(sr.pos, ue.pos)
		return false
	}
	sr.traverseLogEntries(call)
	return nil
}

func (sr *SegmentReader) ReadLogByIndex(index uint64) (*LogEntry, error) {
	pos := int(index - sr.s.Index)
	if pos < 0 || pos > len(sr.pos) {
		return nil, ErrSegmentIndex
	}
	return sr.readOneEntryFrom(sr.pos[pos], false), nil
}

func (sr *SegmentReader) readOneEntryFrom(pos int, copyData bool) *LogEntry {
	le, ok := sr.readLog(pos)
	if ok && copyData {
		data := make([]byte, len(le.Data))
		copy(data, le.Data)
		le.Data = data
	}
	return le
}

func (sr *SegmentReader) FirstIndex() uint64 {
	return sr.s.Index
}

//LastIndex
func (sr *SegmentReader) LastIndex() uint64 {
	return sr.s.Index + uint64(len(sr.pos)) - 1
}

type SegmentProcessor struct {
	buf     *buffer      //缓存
	f       file.WalFile //对应的文件句柄
	s       *Segment     //对应的段信息
	crc32er *crc32er
}

func newSegmentProcessor(s *Segment) *SegmentProcessor {
	return &SegmentProcessor{
		s:       s,
		crc32er: newCrc32er(checkSumPoly),
	}
}

func (sp *SegmentProcessor) open(ft FileType, truncateSize int64) error {
	var (
		f   file.WalFile
		err error
	)
	switch ft {
	case FT_NORMAL:
		f, err = file.NewFile(sp.s.Path)
	case FT_MMAP:
		f, err = file.NewMmapFile(sp.s.Path, file_mmap_size)
	default:
		err = ErrFileTypeNotSupport
	}
	if err != nil {
		return err
	}
	if err = f.Truncate(truncateSize); err != nil {
		return err
	}

	sp.f = f
	return nil
}

func (sp *SegmentProcessor) readToBuffer() error {
	sp.buf = NewBuffer(int(sp.s.Size))
	err := sp.buf.FillFrom(sp.f)
	if err != nil {
		return err
	}
	return nil
}

func (sp *SegmentProcessor) traverseLogEntries(call func(*posEntry) bool) {
	size := sp.buf.Size()
	pos := 0
	for pos < size {
		le, _ := sp.readLog(pos)
		if call(&posEntry{
			LogEntry: le,
			pos:      pos,
		}) {
			return
		}
		pos += le.Len + lenSize
	}
}

func (sp *SegmentProcessor) writeLog(t int8, data []byte) int {
	l := len(data) + crc32Size + typeSize
	totalLen := l + lenSize
	off, _ := sp.buf.Seek(0)
	s := sp.buf.Size() - off
	if totalLen > s {
		sp.buf.Resize(sp.buf.Size() + totalLen - s)
	}
	sp.serializateUint32(sp.buf.Next(lenSize), uint32(l))
	sp.serializateUint32(sp.buf.Next(crc32Size), sp.crc32er.Checksum(data))
	sp.buf.WriteByte(byte(t))
	sp.buf.Write(data)
	return totalLen
}

func (sp *SegmentProcessor) readLog(pos int) (_ *LogEntry, ok bool) {
	lbz := sp.buf.ReadAt(pos, lenSize)
	if lbz == nil {
		return
	}
	l := int(sp.deserializeUint32(lbz))
	data := sp.buf.ReadAt(pos+lenSize, l)
	if len(data) == 0 {
		return
	}
	c := sp.deserializeUint32(data)
	return &LogEntry{
		Len:   l,
		Crc32: c,
		Typ:   int8(data[crc32Size]),
		Data:  data[crc32Size+typeSize:],
	}, true
}

func (sp *SegmentProcessor) crc32Check(crc32 uint32, data []byte) bool {
	return sp.crc32er.Checksum(data) == crc32
}

func (sp *SegmentProcessor) Close() error {
	if sp.f != nil {
		return sp.f.Close()
	}
	return nil
}

func (sp *SegmentProcessor) serializateUint32(b []byte, v uint32) {
	binary.BigEndian.PutUint32(b, v)
}

func (sp *SegmentProcessor) deserializeUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func (sp *SegmentProcessor) Truncate(n int) error {
	err := sp.buf.Truncate(n)
	if err != nil {
		return err
	}
	_, err = sp.f.Seek(int64(n), io.SeekStart)
	return err
}
