package lws

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	defaultOpts = Options{
		SegmentSize:   1 << 26,
		Ft:            FileTypeMmap,
		FileExtension: "wal",
	}
	ErrPurgeWorkExisted = errors.New("purge work has been performed")
	ErrPurgeNotReached  = errors.New("purge threshold not reached")

	InitID    = 1
	InitIndex = 1
)

type PurgeMod int

const (
	PurgeModAsync PurgeMod = 0
	PurgeModSync  PurgeMod = 1
)

type entry struct {
	len   int
	crc32 uint32
	typ   int
	data  []byte
}

type Wal struct {
	mu               sync.Mutex
	path             string //文件路径
	opts             Options
	sw               *SegmentWriter //日志写入器
	currentSegmentID uint64         //最新段/文件ID
	firstIndex       uint64
	lastIndex        uint64
	segmentRW        sync.RWMutex
	segments         []*Segment
	readCache        ReaderCache //缓存，用于缓存读过的数据，采用lru&时间清除
	cond             *sync.Cond
	readCount        int //用于记录当前进行的日志访问者，如果有读者，清理数据的时候要等待读取完成
	purging          int32
}

func Open(path string, opt ...Opt) (*Wal, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	wal := &Wal{
		path: absPath,
		opts: defaultOpts,
		cond: sync.NewCond(&sync.Mutex{}),
	}
	if err = wal.open(opt...); err != nil {
		return nil, err
	}
	return wal, nil
}

func (l *Wal) open(opt ...Opt) error {
	var (
		err error
	)
	for _, o := range opt {
		o(&l.opts)
	}
	if err = l.buildSegments(); err != nil {
		return err
	}
	if len(l.segments) == 0 {
		l.currentSegmentID = 1
		l.lastIndex = 1
		l.segments = append(l.segments, &Segment{
			ID:    uint64(InitID),
			Index: uint64(InitIndex),
			Path:  filepath.Join(l.path, l.segmentName()),
		})
	}
	currentSegment := l.segments[len(l.segments)-1]
	l.currentSegmentID = currentSegment.ID
	l.sw, err = NewSegmentWriter(currentSegment, l.opts.SegmentSize, l.opts.Ft, l.opts.Fs)
	if err != nil {
		return err
	}
	l.lastIndex = currentSegment.Index + uint64(l.sw.EntryCount()-1)
	l.firstIndex = l.segments[0].Index

	return nil
}

func (l *Wal) buildSegments() error {
	if err := os.MkdirAll(l.path, 0777); err != nil {
		return err
	}
	names, err := filepath.Glob(filepath.Join(l.path, fmt.Sprintf("%s*.%s", l.opts.FilePrefix, l.opts.FileExtension)))
	if err != nil {
		return err
	}
	sort.Strings(names)
	l.segmentRW.Lock()
	defer l.segmentRW.Unlock()
	l.segments = make([]*Segment, len(names))
	for i, name := range names {
		id, index, err := l.parseSegmentName(path.Base(name))
		if err != nil {
			return err
		}
		l.segments[i] = &Segment{
			ID:    id,
			Index: index,
			Path:  name,
			Size:  l.fileSize(name),
		}
	}
	return nil
}

func (l *Wal) fileSize(file string) uint64 {
	s, err := os.Stat(file)
	if err != nil {
		return 0
	}
	return uint64(s.Size())
}

func (l *Wal) rollover() error {
	l.currentSegmentID++
	s := &Segment{
		ID:    l.currentSegmentID,
		Index: l.lastIndex,
		Path:  filepath.Join(l.path, l.segmentName()),
	}
	l.segmentRW.Lock()
	l.segments = append(l.segments, s)
	l.segmentRW.Unlock()
	return l.sw.Replace(s)
}

func (l *Wal) segmentName() string {
	return fmt.Sprintf("%s%05d_%d.%s", l.opts.FilePrefix, l.currentSegmentID, l.lastIndex, l.opts.FileExtension)
}

func (l *Wal) parseSegmentName(name string) (id uint64, index uint64, err error) {
	ss := strings.Split(name[len(l.opts.FilePrefix):], "_")
	id, err = strconv.ParseUint(ss[0], 10, 64)
	if err != nil {
		return
	}
	index, err = strconv.ParseUint(strings.Trim(ss[1], "."+l.opts.FileExtension), 10, 64)
	return
}

func (l *Wal) Write(typ int8, obj interface{}) error {
	data, err := l.encodeObj(typ, obj)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.sw.Size() > l.opts.SegmentSize {
		if err := l.rollover(); err != nil {
			return err
		}
	}
	_, err = l.sw.Write(typ, data)
	if err == nil {
		l.lastIndex++
	}
	return err
}

func (l *Wal) encodeObj(t int8, obj interface{}) ([]byte, error) {
	data, ok := obj.([]byte)
	if !ok {
		coder, err := GetCoder(t)
		if err != nil {
			return nil, err
		}
		data, err = coder.Encode(obj)
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (l *Wal) NewLogIterator() *EntryIterator {
	l.readRequest()
	return newEntryIterator(
		&walContainer{
			wal:   l,
			first: l.firstIndex,
			last:  l.lastIndex,
		},
	)
}

func (l *Wal) Flush() error {
	return l.sw.Flush()
}
func (l *Wal) Purge(mod PurgeMod) error {
	switch mod {
	case PurgeModAsync:
		go l.purge()
	case PurgeModSync:
		return l.purge()
	}
	return nil
}
func (l *Wal) WriteToFile(file string, typ int8, obj interface{}) error {
	data, err := l.encodeObj(typ, obj)
	if err != nil {
		return err
	}
	//先检测下file是不是符合wal规范
	sw, err := NewSegmentWriter(&Segment{
		Path: file,
	}, uint64(len(data)+metaSize), FileTypeNormal, FlushStrategySync)
	if err != nil {
		return err
	}
	_, err = sw.Write(typ, data)
	return err
}
func (l *Wal) ReadFromFile(file string) (*EntryIterator, error) {
	sr, err := NewSegmentReader(&Segment{
		Path: file,
	}, FileTypeNormal)
	if err != nil {
		return nil, err
	}
	return newEntryIterator(
		&fileContainer{
			SegmentReader: sr,
		}), nil
}

func (l *Wal) findReaderByIndex(idx uint64) (*refReader, error) {
	s := l.findSegmentByIndex(idx)
	if s == nil {
		return nil, errors.New("idx out of range")
	}
	rr := l.readCache.GetReader(s.ID)
	if rr != nil {
		return rr, nil
	}
	sr, err := NewSegmentReader(s, FileTypeMmap)
	if err != nil {
		return nil, err
	}
	rr = &refReader{
		SegmentReader: sr,
	}
	l.readCache.PutReader(s.ID, rr)
	return rr, nil
}

func (l *Wal) findSegmentByIndex(idx uint64) *Segment {
	l.segmentRW.RLock()
	defer l.segmentRW.RUnlock()
	b, e := 0, len(l.segments)
	for b < e {
		m := (e + b) / 2
		if idx >= l.segments[m].Index {
			b = m + 1
		} else {
			e = m
		}
	}
	return l.segments[b-1]
}

func (l *Wal) purge() error {
	if !atomic.CompareAndSwapInt32(&l.purging, 0, 1) {
		return ErrPurgeWorkExisted
	}
	defer atomic.StoreInt32(&l.purging, 0)
	var (
		dirty      []*Segment
		firstIndex uint64
	)
	l.cond.L.Lock()
	for l.readCount > 0 {
		l.cond.Wait()
	}
	switch l.purgeType() {
	case 0:
	case 1:
		dirty, firstIndex = l.pureWithEntryCount()
	case 2:
		dirty, firstIndex = l.pureWithFileCount()
	}
	if dirty != nil {
		l.firstIndex = firstIndex
	}
	l.cond.L.Unlock()
	if dirty == nil {
		return nil
	}
	for _, s := range dirty {
		l.readCache.DeleteReader(s.ID)
		os.Remove(s.Path)
	}
	return nil
}

func (l *Wal) purgeType() int {
	trigger := l.opts.LogEntryCountLimitForPurge > 0 && l.lastIndex-l.firstIndex > uint64(l.opts.LogEntryCountLimitForPurge)
	if trigger {
		return 1
	}
	trigger = l.opts.LogFileLimitForPurge > 0 && len(l.segments) > l.opts.LogFileLimitForPurge
	if trigger {
		return 2
	}
	return 0
}

func (l *Wal) pureWithEntryCount() (dirty []*Segment, first uint64) {
	threshold := l.lastIndex - uint64(l.opts.LogEntryCountLimitForPurge)
	var (
		i = 0
	)
	l.segmentRW.RLock()
	defer l.segmentRW.RUnlock()
	for ; i < len(l.segments); i++ {
		if l.segments[i].Index > threshold {
			i--
			break
		}
	}
	if i < 0 {
		return
	}
	first, dirty, l.segments = l.segments[i].Index, l.segments[:i], l.segments[i:]
	return
}

func (l *Wal) pureWithFileCount() (dirty []*Segment, first uint64) {
	l.segmentRW.RLock()
	defer l.segmentRW.RUnlock()
	i := len(l.segments) - l.opts.LogFileLimitForPurge
	first, dirty, l.segments = l.segments[i].Index, l.segments[:i], l.segments[i:]
	return
}

func (l *Wal) readRequest() {
	l.cond.L.Lock()
	l.readCount++
	l.cond.L.Unlock()
}

func (l *Wal) readRelease() {
	l.cond.L.Lock()
	l.readCount--
	l.cond.L.Unlock()
	if l.readCount == 0 {
		l.cond.Broadcast()
	}
}
