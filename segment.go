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
	checkSumPoly  = crc32.IEEE
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
	Size  int64  //文件当前大小
	Index uint64 //文件中日志的最小索引
	Path  string //文件路径
}

type crc32Ctor struct {
	poly  uint32
	table *crc32.Table
}

func (crc *crc32Ctor) Checksum(data []byte) uint32 {
	return crc32.Checksum(data, crc.table)
}

func newCrc32er(poly uint32) *crc32Ctor {
	return &crc32Ctor{
		poly:  poly,
		table: crc32.MakeTable(poly),
	}
}

type SegmentWriter struct {
	*SegmentProcessor
	s           *Segment
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
	SegmentSize int64
	Ft          FileType
	Wf          WriteFlag
	Fv          int
	MapLock     bool
	BufferSize  int
}

func NewSegmentWriter(s *Segment, opt WriterOptions) (*SegmentWriter, error) {
	sw := &SegmentWriter{
		SegmentProcessor: newSegmentProcessor(procConfig{
			segmentSize: opt.SegmentSize,
			mapLock:     opt.MapLock,
			bufferSize:  opt.BufferSize,
			ft:          opt.Ft,
		}),
		s:           s,
		ft:          opt.Ft,
		wf:          opt.Wf,
		segmentSize: int(opt.SegmentSize),
		threshold:   opt.Fv,
		closeCh:     make(chan struct{}),
	}
	//打开写入的目标文件
	if err := sw.open(s); err != nil {
		return nil, err
	}
	//遍历文件的所有log entry并检测其完整性
	if err := sw.readAndCheck(); err != nil {
		sw.Close()
		return nil, err
	}
	//如果配置定时刷盘策略，则开启后台刷盘任务
	sw.startFlushWorker()
	return sw, nil
}

func (sw *SegmentWriter) readAndCheck() (err error) {
	//遍历文件中所有的日志条目，如果遍历到文件末尾或者检测到日志损坏，则终止遍历，并从最新的完整条目处开始写日志
	sw.traverseLogEntries(func(ue *posEntry) bool {
		if ue.LogEntry == nil || ue.Len == 0 || !sw.crc32Check(ue.Crc32, ue.Data) {
			// sw.f.Truncate(int64(ue.pos))
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

//flushTimeDelay 后台刷新程序，定时驱动，默认为1s，如果检测到有已经写入但未同步的条目，则进行刷盘
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

//EntryCount 返回当前文件写入的总条目数
func (sw *SegmentWriter) EntryCount() int {
	return sw.count
}

//Replace 根据s信息替换写入的文件，即文件切换
//切换前会将老数据进行刷盘，并将文件大小调整到实际写入大小，然后打开一个新的文件，并替换老文件，如果打开出错，则保持老文件
func (sw *SegmentWriter) Replace(s *Segment) error {
	if sw.s.ID == s.ID {
		return nil
	}
	if err := sw.Flush(); err != nil {
		return err
	}
	sw.truncate()
	if err := sw.open(s); err != nil {
		return err
	}
	sw.s = s
	sw.count = 0
	return nil
}

func (sw *SegmentWriter) Write(t int8, data []byte) (int, error) {
	sw.writeLocker.Lock()
	l, err := sw.writeToBuffer(t, data) //蒋日志写入缓存中，如果写入失败，则回退写入游标，以防止用户重试时数据出现错乱
	if err != nil {
		sw.f.Seek(int64(-l), io.SeekCurrent)
		sw.writeLocker.Unlock()
		return 0, err
	}
	//如果指定了写缓存的同时，写文件，则将缓存回写到文件中，写入失败，则将游标回退，以防止用户重试时数据出现错乱
	if sw.wf&WF_SYNCWRITE == WF_SYNCWRITE {
		if err := sw.f.WriteBack(); err != nil {
			sw.f.Seek(int64(-l), io.SeekCurrent)
			sw.writeLocker.Unlock()
			return 0, err
		}
	}
	sw.acc++
	sw.writeLocker.Unlock()
	sw.tryFlush() //检测是否需要进行刷盘操作
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
	//如果用户指定了按照写入日志条目累计数进行刷盘，则检测
	if sw.wf&WF_QUOTAFLUSH == WF_QUOTAFLUSH && sw.acc >= sw.threshold {
		return sw.Flush()
	}
	return nil
}

//Size 获取文件当前的写入的大小，因为writer会预分配文件大小，所以使用write offset标识写入的大小值
func (sw *SegmentWriter) Size() int64 {
	n, _ := sw.f.Seek(0, io.SeekCurrent)
	return n
}

//Flush 如果用户没有指定同步写文件操作，则需要将缓存数据回写到文件，再进行刷盘
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

//truncate将文件大小调整至实际内容大小
func (sw *SegmentWriter) truncate() error {
	n, _ := sw.f.Seek(0, io.SeekCurrent)
	return sw.f.Truncate(n)
}

func (sw *SegmentWriter) Close() error {
	close(sw.closeCh)
	sw.truncate()
	return sw.SegmentProcessor.Close()
}

type SegmentReader struct {
	*SegmentProcessor
	s   *Segment
	pos []int //记录每个entry的起始位置
}

func NewSegmentReader(s *Segment, ft FileType) (*SegmentReader, error) {
	var (
		sr = &SegmentReader{
			SegmentProcessor: newSegmentProcessor(procConfig{
				segmentSize: s.Size,
				bufferSize:  -1,
				ft:          ft,
			}),
			s: s,
		}
		err error
	)

	if err = sr.open(s); err != nil {
		return nil, err
	}

	if err = sr.loadEntries(); err != nil {
		sr.Close()
		return nil, err
	}
	return sr, nil
}

//loadEntries 遍历文件中所有的日志条目直至文件末尾或出现日志损坏处，将遍历的条目所在文件的pos记录在案
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

//ReadLogByIndex 通过index获取到指定的日志条目
func (sr *SegmentReader) ReadLogByIndex(index uint64) (*LogEntry, error) {
	pos := int(index - sr.s.Index) //通过index与文件中起始条目的index差值，获取到索引值，通过索引值获取到日志在文件的位置，并读取
	if pos < 0 || pos >= len(sr.pos) {
		return nil, ErrSegmentIndex
	}
	return sr.readOneEntryFrom(sr.pos[pos], false), nil
}

//readOneEntryFrom 从文件的pos处读取一个entry，copyData标识读取的日志是否需要copy，在有缓存层的情况下，读出的数据是缓存层的一部分，建议进行copy
//因为缓存层会进行复用，即覆盖历史数据，异或上层用户会修改数据以影响到缓存层
func (sr *SegmentReader) readOneEntryFrom(pos int, copyData bool) *LogEntry {
	le, err := sr.readLog(int64(pos))
	if err == nil && copyData {
		data := make([]byte, len(le.Data))
		copy(data, le.Data)
		le.Data = data
	}
	return le
}

//FirstIndex 此文件段条目的起始索引
func (sr *SegmentReader) FirstIndex() uint64 {
	return sr.s.Index
}

//LastIndex 此文件段条目的结束索引
func (sr *SegmentReader) LastIndex() uint64 {
	return sr.s.Index + uint64(len(sr.pos)) - 1
}

type SegmentProcessor struct {
	f       *logfile
	pc      procConfig //对应的段信息
	crc32er *crc32Ctor
}

type procConfig struct {
	segmentSize int64    //文件预留大小
	mapLock     bool     //内存映射使是否进行内存锁定以提高write性能
	bufferSize  int      //缓存大小
	ft          FileType //文件类型
}

func newSegmentProcessor(pc procConfig) *SegmentProcessor {
	return &SegmentProcessor{
		pc:      pc,
		crc32er: newCrc32er(checkSumPoly), //生成crc计算器
	}
}

func (sp *SegmentProcessor) open(s *Segment) error {
	//用户指定了缓存大小，则使用指定的大小，如果为0，则不使用缓存，如果指定的bufferSize<0则使用系统自分配缓存, 首先根据文件预大小使用同等大小的缓存，如果文件没有预大小，则使用默认的缓存大小（128M)
	//如果预分配过大，则调整到最大缓存512M
	bufsz := sp.pc.bufferSize
	if bufsz < 0 {
		bufsz = int(sp.pc.segmentSize)
		if bufsz == 0 {
			bufsz = bufferSize
		} else if bufsz > maxBufferSize {
			bufsz = maxBufferSize
		}
	}
	//创建一个新的日志文件
	f, err := newLogFile(s.Path, sp.pc.ft, sp.pc.segmentSize, bufsz, sp.pc.mapLock)
	if err != nil {
		return err
	}
	//如果processor有老的日志文件，则关闭此文件
	if sp.f != nil {
		sp.f.Close()
	}
	sp.f = f
	return nil
}

//traverseLogEntries processor会遍历读取文件中的日志，并回调call函数，call返回true则代表终止遍历
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

//crc32Check检测data的crc32值和传入的crc32值是否相等
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
