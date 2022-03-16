/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package lws

import (
	"errors"
	"hash/crc32"
	"io"
	"sync"
	"time"
)

const (
	checkSumPoly  = 0xD5828281
	bufferSize    = 1 << 27
	maxBufferSize = 1 << 29

	lenSize   = 4
	crc32Size = 4
	typeSize  = 1
	metaSize  = lenSize + crc32Size + typeSize
)

var (
	ErrFileTypeNotSupport = errors.New("this file type is not supported")
	ErrSegmentIndex       = errors.New("index out of segment range")
)

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
	segmentSize int
	count       int //写入条目的数量
	closeCh     chan struct{}
	writeLocker sync.Mutex //非同步写情况下，可能会导致并发写相同数据
}

type WriterOptions struct {
	SegmentSize uint64
	Ft          FileType
	Wf          WriteFlag
	Fv          int
	MapLock     bool
	BufferSize  int
}

func NewSegmentWriter(s *Segment, opt WriterOptions) (*SegmentWriter, error) {
	sw := &SegmentWriter{
		SegmentProcessor: newSegmentProcessor(procConfig{
			Segment:     s,
			segmentSize: int(opt.SegmentSize),
			mapLock:     opt.MapLock,
			bufferSize:  opt.BufferSize,
		}),
		ft:          opt.Ft,
		wf:          opt.Wf,
		segmentSize: int(opt.SegmentSize),
		threshold:   opt.Fv,
		closeCh:     make(chan struct{}),
	}
	if err := sw.open(); err != nil {
		return nil, err
	}
	if err := sw.readAndCheck(); err != nil {
		sw.Close()
		return nil, err
	}

	if sw.f.Size() < int64(sw.segmentSize) {
		if err := sw.f.Truncate(int64(sw.segmentSize)); err != nil {
			sw.Close()
			return nil, err
		}
	}

	sw.startFlushWorker()
	return sw, nil
}

func (sw *SegmentWriter) open() error {
	return sw.SegmentProcessor.open(sw.ft)
}

func (sw *SegmentWriter) readAndCheck() (err error) {
	sw.traverseLogEntries(func(ue *posEntry) bool {
		if ue.LogEntry == nil || ue.Len == 0 || !sw.crc32Check(ue.Crc32, ue.Data) {
			sw.f.Truncate(int64(ue.pos))
			sw.f.Seek(int64(ue.pos), io.SeekStart)
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
	f, old := sw.f, sw.s
	sw.s = old
	sw.s.Segment = s
	if err := sw.open(); err != nil {
		sw.s = old
		return err
	}
	f.Close()
	sw.count = 0
	return nil
}

func (sw *SegmentWriter) Write(t int8, data []byte) (int, error) {
	sw.writeLocker.Lock()
	l, err := sw.writeToBuffer(t, data)
	if err != nil {
		sw.f.Seek(int64(-l), io.SeekCurrent)
		sw.writeLocker.Unlock()
		return 0, err
	}

	if sw.wf&WF_SYNCWRITE == WF_SYNCWRITE {
		if err := sw.f.WriteBack(); err != nil {
			sw.f.Seek(int64(-l), io.SeekCurrent)
			sw.writeLocker.Unlock()
		}
	}
	sw.writeLocker.Unlock()
	sw.tryFlush()
	return len(data), err
}

func (sw *SegmentWriter) writeToBuffer(t int8, data []byte) (int, error) {
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

func (sw *SegmentWriter) Size() int64 {
	return sw.f.Size()
}

func (sw *SegmentWriter) Flush() error {
	if sw.wf&WF_SYNCWRITE != WF_SYNCWRITE {
		sw.writeLocker.Lock()
		err := sw.f.WriteBack()
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
			SegmentProcessor: newSegmentProcessor(procConfig{
				Segment:     s,
				segmentSize: int(s.Size),
				bufferSize:  -1,
			}),
		}
		err error
	)

	if err = sr.open(ft); err != nil {
		return nil, err
	}

	if err = sr.loadEntries(); err != nil {
		sr.Close()
		return nil, err
	}
	return sr, nil
}

func (sr *SegmentReader) loadEntries() error {
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
	if pos < 0 || pos >= len(sr.pos) {
		return nil, ErrSegmentIndex
	}
	return sr.readOneEntryFrom(sr.pos[pos], false), nil
}

func (sr *SegmentReader) readOneEntryFrom(pos int, copyData bool) *LogEntry {
	le, err := sr.readLog(int64(pos))
	if err == nil && copyData {
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
	f       *logfile
	s       procConfig //对应的段信息
	crc32er *crc32er
	mapLock bool
}

type procConfig struct {
	*Segment
	segmentSize int
	mapLock     bool
	bufferSize  int
}

func newSegmentProcessor(s procConfig) *SegmentProcessor {
	return &SegmentProcessor{
		s:       s,
		crc32er: newCrc32er(checkSumPoly),
		mapLock: s.mapLock,
	}
}

func (sp *SegmentProcessor) open(ft FileType) error {
	bufsz := sp.s.bufferSize
	if bufsz < 0 {
		if sp.s.segmentSize > 0 {
			bufsz = sp.s.segmentSize
		} else {
			bufsz = bufferSize
		}
	}
	if bufsz > maxBufferSize {
		bufsz = maxBufferSize
	}

	f, err := newLogFile(sp.s.Path, ft, bufsz, sp.s.mapLock)
	if err != nil {
		return err
	}
	sp.f = f
	return nil
}

func (sp *SegmentProcessor) traverseLogEntries(call func(*posEntry) bool) {
	pos := 0
	for {
		le, _ := sp.readLog(int64(pos))
		if call(&posEntry{
			LogEntry: le,
			pos:      pos,
		}) {
			return
		}
		pos += le.Len + lenSize
	}
}

func (sp *SegmentProcessor) writeLog(t int8, data []byte) (int, error) {
	return sp.f.WriteLog(t, data, sp.crc32er.Checksum(data))
}

func (sp *SegmentProcessor) readLog(pos int64) (*LogEntry, error) {
	return sp.f.ReadLog(pos)
}

func (sp *SegmentProcessor) crc32Check(crc32 uint32, data []byte) bool {
	return sp.crc32er.Checksum(data) == crc32
}

func (sp *SegmentProcessor) closeFile() error {
	if sp.f != nil {
		if err := sp.f.Close(); err != nil {
			return err
		}
		sp.f = nil
	}
	return nil
}

func (sp *SegmentProcessor) Close() error {
	return sp.closeFile()
}
