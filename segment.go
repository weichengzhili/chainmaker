package lws

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"time"

	"chainmaker.org/chainmaker/lws/file"
)

const (
	file_mmap_size = 1 << 26
	checkSumPoly   = 0xD5828281
	bufferSize     = 1 << 26
	entryMetaLen   = 8

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
	*SegmenterProcessor
	ft          FileType
	fs          FlushStrategy //刷盘策略 flushStrategy
	threshold   int
	acc         int //等待刷盘的累计值
	segmentSize uint64
	count       int //写入条目的数量
}

func NewSegmentWriter(s *Segment, segmentSize uint64, ft FileType, fs FlushStrategy) (*SegmentWriter, error) {
	sw := &SegmentWriter{
		SegmenterProcessor: newSegmentProcessor(s),
		ft:                 ft,
		fs:                 fs,
		segmentSize:        segmentSize,
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
	return sw.SegmenterProcessor.open(sw.ft, int64(sw.segmentSize))
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
	switch sw.fs {
	case FlushStrategyTimeDelay:
		go sw.flushTimeDelay()
	}
}

func (sw *SegmentWriter) flushTimeDelay() {
	t := time.Millisecond * time.Duration(sw.threshold)
	timer := time.NewTimer(t)
	for {
		<-timer.C
		// sw.buf.Dredge()
		// sw.buf.Flush()
		sw.buf.FlushTo(sw.f)
		timer.Reset(t)
	}
}

func (sw *SegmentWriter) EntryCount() int {
	return sw.count
}

func (sw *SegmentWriter) Replace(s *Segment) error {
	if sw.s.ID == s.ID {
		return nil
	}
	if _, err := sw.buf.FlushTo(sw.f); err != nil {
		return err
	}
	sw.s = s
	if err := sw.open(); err != nil {
		return err
	}
	sw.buf.Reset()
	return nil
}

func (sw *SegmentWriter) Write(t int8, data []byte) (int, error) {
	l := sw.writeToBuffer(t, data)
	err := sw.tryFlush(l)
	if err != nil {
		return 0, nil
	}
	return l, nil
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
	return n1 + n2, sw.f.Flush()
}

func (sw *SegmentWriter) writeToBuffer(t int8, data []byte) int {
	return sw.writeLog(t, data)
}

func (sw *SegmentWriter) tryFlush(length int) error {
	writeAndFlush := func() error {
		if _, err := sw.buf.WriteTo(sw.f); err != nil {
			sw.buf.Seek(-length)
			return err
		}
		//flush可以不做检测吗？我认为可以
		if err := sw.f.Flush(); err != nil {
			return err
		}
		return nil
	}
	switch sw.fs {
	case FlushStrategySync:
		return writeAndFlush()
	case FlushStrategyCapDelay:
		sw.acc += length
		if sw.acc >= sw.threshold {
			return writeAndFlush()
		}
	case FlushStrategyQuantityDelay:
		sw.acc++
		if sw.acc >= sw.threshold {
			return writeAndFlush()
		}
	}
	return nil
}

func (sw *SegmentWriter) Size() uint64 {
	return uint64(sw.buf.Size())
}

func (sw *SegmentWriter) Flush() (err error) {
	_, err = sw.buf.FlushTo(sw.f)
	return err
}

func (sw *SegmentWriter) Close() error {
	return sw.SegmenterProcessor.close()
}

type SegmentReader struct {
	*SegmenterProcessor
	pos []int //记录每个entry的起始位置
}

func NewSegmentReader(s *Segment, ft FileType) (*SegmentReader, error) {
	var (
		sr = &SegmentReader{
			SegmenterProcessor: newSegmentProcessor(s),
		}
		err error
	)

	if err = sr.open(FileTypeMmap, 0); err != nil {
		return nil, err
	}

	if err = sr.loadEntries(); err != nil {
		sr.close()
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
	return sr.readOneEntryFrom(sr.pos[pos]), nil
}

func (sr *SegmentReader) readOneEntryFrom(pos int) *LogEntry {
	return sr.readLog(pos)
}

func (sr *SegmentReader) FirstIndex() uint64 {
	return sr.s.Index
}

func (sr *SegmentReader) LastIndex() uint64 {
	return sr.s.Index + uint64(len(sr.pos))
}

func (sr *SegmentReader) close() {
	if sr.f != nil {
		sr.f.Close()
	}
}

type SegmenterProcessor struct {
	buf     *buffer      //缓存
	f       file.WalFile //对应的文件句柄
	s       *Segment     //对应的段信息
	crc32er *crc32er
}

func newSegmentProcessor(s *Segment) *SegmenterProcessor {
	return &SegmenterProcessor{
		s:       s,
		crc32er: newCrc32er(checkSumPoly),
	}
}

func (sp *SegmenterProcessor) open(ft FileType, truncateSize int64) error {
	var (
		f   file.WalFile
		err error
	)
	switch ft {
	case FileTypeNormal:
		f, err = file.NewFile(sp.s.Path, truncateSize)
	case FileTypeMmap:
		f, err = file.NewMmapFile(sp.s.Path, file_mmap_size, truncateSize)
	default:
		err = ErrFileTypeNotSupport
	}
	if err != nil {
		return err
	}
	sp.f = f
	return nil
}

func (sp *SegmenterProcessor) readToBuffer() error {
	sp.buf = NewBuffer(int(sp.s.Size))
	err := sp.buf.FillFrom(sp.f)
	if err != nil {
		return err
	}
	return nil
}

func (sp *SegmenterProcessor) traverseLogEntries(call func(*posEntry) bool) {
	size := sp.buf.Size()
	pos := 0
	for pos < size {
		le := sp.readLog(pos)
		if call(&posEntry{
			LogEntry: le,
			pos:      pos,
		}) {
			return
		}
		pos += le.Len + lenSize
	}
}

func (sp *SegmenterProcessor) writeLog(t int8, data []byte) int {
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

func (sp *SegmenterProcessor) readLog(pos int) *LogEntry {
	lbz := sp.buf.ReadAt(pos, lenSize)
	if lbz == nil {
		return nil
	}
	l := int(sp.deserializeUint32(lbz))
	data := sp.buf.ReadAt(pos+lenSize, l)
	if len(data) == 0 {
		return nil
	}
	c := sp.deserializeUint32(data)
	cp := make([]byte, len(data)-crc32Size-typeSize)
	copy(cp, data[crc32Size+typeSize:])
	return &LogEntry{
		Len:   l,
		Crc32: c,
		Typ:   int8(data[crc32Size]),
		Data:  cp,
	}
}

func (sp *SegmenterProcessor) crc32Check(crc32 uint32, data []byte) bool {
	return sp.crc32er.Checksum(data) == crc32
}

func (sp *SegmenterProcessor) close() error {
	if sp.f != nil {
		return sp.f.Close()
	}
	return nil
}

func (sp *SegmenterProcessor) serializateUint32(b []byte, v uint32) {
	binary.BigEndian.PutUint32(b, v)
}

func (sp *SegmenterProcessor) deserializeUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func (sp *SegmenterProcessor) Truncate(n int) error {
	err := sp.buf.Truncate(n)
	if err != nil {
		return err
	}
	_, err = sp.f.Seek(int64(n), io.SeekStart)
	return err
}
