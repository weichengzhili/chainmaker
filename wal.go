package wal

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

var (
	defaultOpts = Options{
		SegmentSize:   1 << 26,
		Ft:            FileTypeMmap,
		FileExtension: "wal",
	}
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
	lastIndex        uint64
	segments         []Segment
	//readCache   cache //缓存，用于缓存读过的数据，采用lru&时间清除
	readerCount int //用于记录当前进行的日志访问者，如果有读者，清理数据的时候要等待读取完成
}

func Open(path string, opt ...Opt) (*Wal, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
	}

	wal := &Wal{
		path: absPath,
		opts: defaultOpts,
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
	currentSegment := l.segments[len(l.segments)]
	l.currentSegmentID = currentSegment.ID
	l.sw, err = NewSegmentWriter(&l.segments[len(l.segments)], l.opts.SegmentSize, l.opts.Ft, l.opts.Fs)
	if err != nil {
		return err
	}
	l.lastIndex = currentSegment.Index + uint64(l.sw.EntryCount())
	return nil
}

func (l *Wal) buildSegments() error {
	names, err := filepath.Glob(filepath.Join(l.path, fmt.Sprintf("%s*.%s", l.opts.FilePrefix, l.opts.FileExtension)))
	if err != nil {
		return err
	}
	sort.Strings(names)
	l.segments = make([]Segment, len(names))
	for i, name := range names {
		id, index, err := l.parseSegmentName(name)
		if err != nil {
			return err
		}
		l.segments[i] = Segment{
			ID:    id,
			Index: index,
			Path:  filepath.Join(l.path, name),
		}
	}
	return nil
}

func (l *Wal) rollover() error {
	l.currentSegmentID++
	s := Segment{
		ID:    l.currentSegmentID,
		Index: l.lastIndex,
		Path:  filepath.Join(l.path, l.segmentName()),
	}

	return l.sw.Replace(&s)
}

func (l *Wal) segmentName() string {
	return fmt.Sprintf("%s%05d_%d.%s", l.opts.FilePrefix, l.currentSegmentID, l.lastIndex, l.opts.FileExtension)
}

func (l *Wal) parseSegmentName(name string) (id uint64, index uint64, err error) {
	id, err = strconv.ParseUint(name[len(l.opts.FilePrefix):], 10, 64)
	if err != nil {
		return
	}
	index, err = strconv.ParseUint(name[len(l.opts.FilePrefix)+6:], 10, 64)
	return
}

func (l *Wal) Write(typ int, data interface{}) error {
	return nil
}

func (l *Wal) NewLogIterator() *EntryIterator {
	return nil
}
func (l *Wal) Flush() error {
	return nil
}
func (l *Wal) Purge(mod int) error {
	return nil
}
func (l *Wal) WriteToFile(file string, typ int, data interface{}) error {
	return nil
}
func (l *Wal) ReadFromFile(file string) *EntryIterator {
	return nil
}
