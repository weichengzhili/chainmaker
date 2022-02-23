package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"

	"chainmaker.org/chainmaker/lws/file"
)

const (
	file_mmap_size = 1 << 26
	checkSumPoly   = 0xD5828281
)

var (
	ErrFileTypeNotSupport = errors.New("this file type is not supported")
	ErrNoEnoughData       = errors.New("no enough data in buffer")
	ErrTruncate           = errors.New("truncation out of range")
)

type WriteFlusher interface {
	Write([]byte) (int, error)
	Flush() error
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
	buf         *buffer      //缓存
	w           file.WalFile //对应的写入的文件
	ft          FileType
	fs          FlushStrategy //刷盘策略 flushStrategy
	s           *Segment      //对应的段信息
	segmentSize uint64
	count       int //写入条目的数量
	crc32er     *crc32er
}

func NewSegmentWriter(s *Segment, segmentSize uint64, ft FileType, fs FlushStrategy) (*SegmentWriter, error) {
	sw := &SegmentWriter{
		ft:          ft,
		fs:          fs,
		s:           s,
		segmentSize: segmentSize,
		crc32er:     newCrc32er(checkSumPoly),
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
	switch sw.ft {
	case FileTypeNormal:
		f, err := file.NewFile(sw.s.Path, int64(sw.segmentSize))
		if err != nil {
			return err
		}
		sw.w = f
	case FileTypeMmap:
		f, err := file.NewMmapFile(sw.s.Path, file_mmap_size, int64(sw.segmentSize))
		if err != nil {
			return err
		}
		sw.w = f
	default:
		return ErrFileTypeNotSupport
	}
	return nil
}

func (sw *SegmentWriter) readAndCheck() error {
	sw.buf = NewBuffer(int(sw.segmentSize))
	err := sw.buf.ReadAllFrom(sw.w)
	if err != nil {
		return err
	}
	for sw.buf.Size() > 0 {
		l := sw.buf.GetUint32()
		crc32 := sw.buf.GetUint32()
		data := sw.buf.Next(int(l))
		if l == 0 || crc32 != sw.crc32er.Checksum(data) {
			return sw.buf.Truncate(0)
		}
		sw.count++
	}
	return nil
}

func (sw *SegmentWriter) startFlushWorker() {

}

func (sw *SegmentWriter) EntryCount() int {
	return sw.count
}

func (sw *SegmentWriter) Replace(s *Segment) error {
	if sw.s.ID == s.ID {
		return nil
	}
	sw.s = s
	if err := sw.open(); err != nil {
		return err
	}
	sw.buf.Reset()
	return nil
}

func (sw *SegmentWriter) Write(data []byte) (int, error) {
	l := len(data)
	sw.buf.PutUint32(uint32(l))
	sw.buf.PutUint32(sw.crc32er.Checksum(data))
	err := sw.buf.Write(data)
	if err != nil {
		return 0, err
	}
	if sw.ft == FileType(FlushStrategySync) {

	}
	return l, nil
}

func (sw *SegmentWriter) Flush() error {
	return nil
}

type buffer struct {
	buf        []byte
	off        int
	wfr        WriteFlusher
	flushPoint uint64
}

func NewBuffer(cap int) *buffer {
	return &buffer{
		buf: make([]byte, 0, cap),
	}
}

func (b *buffer) ReadAllFrom(r io.Reader) error {
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
			return err
		}
	}
}

func (b *buffer) Write(data []byte) error {
	l := len(data)
	if b.shouldGrow(l) {
		b.grow(l)
	}
	copy(b.buf[len(b.buf):], data)
	return nil
}

func (b *buffer) Peek(n int) []byte {
	s := b.Size()
	if s < n {
		n = s
	}
	return b.buf[b.off : b.off+n]
}

func (b *buffer) Next(n int) []byte {
	data := b.Peek(n)
	b.off += n
	return data
}

func (b *buffer) Leap(n int) {
	if n <= b.Size() {
		b.off += n
		return
	}
	b.off = len(b.buf)
}

func (b *buffer) PutUint32(v uint32) {
	if b.shouldGrow(4) {
		b.grow(4)
	}
	l := len(b.buf)
	b.buf = b.buf[:l+4]
	binary.BigEndian.PutUint32(b.buf[l:], v)
}

func (b *buffer) GetUint32() uint32 {
	v := binary.BigEndian.Uint32(b.buf[b.off:])
	b.off += 4
	return v
}

func (b *buffer) Truncate(n int) error {
	if n < -b.off || n > b.Size() {
		return ErrTruncate
	}
	b.buf = b.buf[:b.off+n]
	if n < 0 {
		b.off += n
	}
	return nil
}

func (b *buffer) Reset() {
	b.buf = b.buf[:0]
	b.off = 0
	b.flushPoint = 0
}

func (b *buffer) Dredge() error {
	_, err := b.wfr.Write(b.buf[b.off:])
	return err
}

func (b *buffer) Flush() error {
	return nil
}

func (b *buffer) shouldGrow(n int) bool {
	return n > cap(b.buf)-len(b.buf)
}

func (b *buffer) grow(n int) {

}

func (b *buffer) Size() int {
	return len(b.buf) - b.off
}
