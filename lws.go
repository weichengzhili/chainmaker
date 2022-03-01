/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package lws

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
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
		Fs:            FlushStrategySync,
	}
	fileReg             = `%s\d{5}_\d+\.%s`
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

type Lws struct {
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
	writeNoticeCh    chan writeNoticeType
	closeCh          chan struct{}
}

/*
 @title: Open
 @description: open a new lws instance
 @param {string} path 日志文件存放路径
 @param {...Opt} opt 打开日志写入系统的参数配置
 @return {*Lws} 日志写入系统实例句柄
 @return {error} 错误信息
*/
func Open(path string, opt ...Opt) (*Lws, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	lws := &Lws{
		path:    absPath,
		opts:    defaultOpts,
		cond:    sync.NewCond(&sync.Mutex{}),
		closeCh: make(chan struct{}),
	}
	if err = lws.open(opt...); err != nil {
		return nil, err
	}
	if lws.opts.LogEntryCountLimitForPurge > 0 || lws.opts.LogFileLimitForPurge > 0 {
		lws.writeNoticeCh = make(chan writeNoticeType)
		go lws.cleanStartUp()
	}
	return lws, nil
}

func (l *Lws) open(opt ...Opt) error {
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
	l.lastIndex = currentSegment.Index + uint64(l.sw.EntryCount())
	l.firstIndex = l.segments[0].Index

	if l.opts.LogEntryCountLimitForPurge > 0 || l.opts.LogFileLimitForPurge > 0 {
		go l.purge()
	}

	return nil
}

func (l *Lws) buildSegments() error {
	if err := os.MkdirAll(l.path, 0777); err != nil {
		return err
	}

	names, err := l.matchFiles()
	if err != nil {
		return err
	}
	sort.Strings(names)
	l.segmentRW.Lock()
	defer l.segmentRW.Unlock()
	l.segments = make([]*Segment, len(names))
	for i, name := range names {
		fullPath := path.Join(l.path, name)
		id, index, err := l.parseSegmentName(name)
		if err != nil {
			return err
		}
		l.segments[i] = &Segment{
			ID:    id,
			Index: index,
			Path:  fullPath,
			Size:  l.fileSize(fullPath),
		}
	}
	return nil
}

func (l *Lws) matchFiles() ([]string, error) {
	reg, err := regexp.Compile(fmt.Sprintf(fileReg, l.opts.FilePrefix, l.opts.FileExtension))
	if err != nil {
		return nil, err
	}
	var (
		names []string
	)
	err = filepath.Walk(l.path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			if reg.Match([]byte(info.Name())) {
				names = append(names, info.Name())
			}
		}
		return nil
	})

	return names, err
}

func (l *Lws) fileSize(file string) uint64 {
	s, err := os.Stat(file)
	if err != nil {
		return 0
	}
	return uint64(s.Size())
}

func (l *Lws) rollover() error {
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

func (l *Lws) segmentName() string {
	return fmt.Sprintf("%s%05d_%d.%s", l.opts.FilePrefix, l.currentSegmentID, l.lastIndex, l.opts.FileExtension)
}

func (l *Lws) parseSegmentName(name string) (id uint64, index uint64, err error) {
	ss := strings.Split(name[len(l.opts.FilePrefix):], "_")
	id, err = strconv.ParseUint(ss[0], 10, 64)
	if err != nil {
		return
	}
	index, err = strconv.ParseUint(strings.Trim(ss[1], "."+l.opts.FileExtension), 10, 64)
	return
}

/*
 @title: Write
 @description: 将obj对象写入文件
 @param {int8} typ 写入的数据类型
 @param {interface{}} obj  数据
 @return {error} 成功返回nil，错误返回错误详情
*/
func (l *Lws) Write(typ int8, obj interface{}) error {
	t, data, err := l.encodeObj(typ, obj)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var (
		writeNotice writeNoticeType
	)
	if l.sw.Size() > l.opts.SegmentSize {
		writeNotice |= newFile
		if err := l.rollover(); err != nil {
			return err
		}
	}
	_, err = l.sw.Write(t, data)
	if err == nil {
		writeNotice |= newLog
		l.lastIndex++
	}
	l.writeNotice(writeNotice)
	return err
}

func (l *Lws) encodeObj(t int8, obj interface{}) (int8, []byte, error) {
	data, ok := obj.([]byte)
	if !ok {
		coder, err := GetCoder(t)
		if err != nil {
			return t, nil, err
		}
		data, err = coder.Encode(obj)
		if err != nil {
			return t, nil, err
		}
	} else {
		t = RawCoderType
	}
	return t, data, nil
}

/*
 @title: NewLogIterator
 @description: 对日志写入系统的当前状态生成日志条目迭代器
 @return {*EntryIterator} 日志条目迭代器
*/
func (l *Lws) NewLogIterator() *EntryIterator {
	l.readRequest()
	return newEntryIterator(
		&walContainer{
			wal:   l,
			first: l.firstIndex,
			last:  l.lastIndex - 1,
		},
	)
}

/*
 @title: Flush
 @description: 手动将写入的日志条目强制刷盘
 @return {error} 错误信息
*/
func (l *Lws) Flush() error {
	return l.sw.Flush()
}

/*
 @title: Purge
 @description: 根据配置的清理策略对日志文件进行清理
 @param {PurgeMod} mod:  0异步清理  1:同步清理
 @return {error} 错误信息
*/
func (l *Lws) Purge(mod PurgeMod) error {
	switch mod {
	case PurgeModAsync:
		go l.purge()
	case PurgeModSync:
		return l.purge()
	}
	return nil
}

/*
 @title: WriteToFile
 @description: 将日志写入到特定的文件中，此日志文件名避免跟wal日志文件名冲突
 @param {string} file 文件名
 @param {int8} typ 写入的日志类型
 @param {interface{}} obj 日志数据
 @return {error} 错误信息
*/
func (l *Lws) WriteToFile(file string, typ int8, obj interface{}) error {
	reg, err := regexp.Compile(fmt.Sprintf(fileReg, l.opts.FilePrefix, l.opts.FileExtension))
	if err != nil {
		return err
	}
	if reg.Match([]byte(file)) {
		return errors.New("the file name is invalid: file name cant match with wal file")
	}
	t, data, err := l.encodeObj(typ, obj)
	if err != nil {
		return err
	}
	//先检测下file是不是符合wal规范
	sw, err := NewSegmentWriter(&Segment{
		Path: path.Join(l.path, file),
	}, 0, FileTypeNormal, FlushStrategySync)
	if err != nil {
		return err
	}
	_, err = sw.Write(t, data)
	return err
}

func (l *Lws) ReadFromFile(file string) (*EntryIterator, error) {
	sr, err := NewSegmentReader(&Segment{
		Path:  path.Join(l.path, file),
		Index: 1,
	}, FileTypeNormal)
	if err != nil {
		return nil, err
	}
	return newEntryIterator(
		&fileContainer{
			SegmentReader: sr,
		}), nil
}

func (l *Lws) findReaderByIndex(idx uint64) (*refReader, error) {
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

func (l *Lws) findSegmentByIndex(idx uint64) *Segment {
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

func (l *Lws) purge() error {
	if !atomic.CompareAndSwapInt32(&l.purging, 0, 1) {
		return ErrPurgeWorkExisted
	}
	defer atomic.StoreInt32(&l.purging, 0)
	typ := l.purgeType()
	if typ == 0 {
		return nil
	}
	var (
		dirty      []*Segment
		firstIndex uint64
	)
	l.cond.L.Lock()
	for l.readCount > 0 {
		l.cond.Wait()
	}

	switch typ {
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
		if rd := l.readCache.DeleteReader(s.ID); rd != nil {
			rd.Close()
		}
		os.Remove(s.Path)
	}
	return nil
}

func (l *Lws) purgeType() int {
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

func (l *Lws) pureWithEntryCount() (dirty []*Segment, first uint64) {
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

func (l *Lws) pureWithFileCount() (dirty []*Segment, first uint64) {
	l.segmentRW.RLock()
	defer l.segmentRW.RUnlock()
	i := len(l.segments) - l.opts.LogFileLimitForPurge
	if i <= 0 {
		return
	}
	first, dirty, l.segments = l.segments[i].Index, l.segments[:i], l.segments[i:]
	return
}

func (l *Lws) readRequest() {
	l.cond.L.Lock()
	l.readCount++
	l.cond.L.Unlock()
}

func (l *Lws) readRelease() {
	l.cond.L.Lock()
	l.readCount--
	l.cond.L.Unlock()
	if l.readCount == 0 {
		l.cond.Broadcast()
	}
}

func (l *Lws) writeNotice(nt writeNoticeType) {
	select {
	case l.writeNoticeCh <- nt:
	default:
	}
}

func (l *Lws) cleanStartUp() {
	var (
		fileCount  int
		entryCount uint64
		reassign   = func() {
			fileCount = len(l.segments)
			entryCount = l.lastIndex - l.firstIndex
		}
	)
	reassign()
	for {
		select {
		case t := <-l.writeNoticeCh:
			if t&newLog != 0 {
				entryCount++
			}
			if t&newFile != 0 {
				fileCount++
			}
			if entryCount > uint64(l.opts.LogEntryCountLimitForPurge) || fileCount > l.opts.LogFileLimitForPurge {
				l.purge()
				reassign()
			}
		case <-l.closeCh:
			return
		}
	}
}

func (l *Lws) Close() {
	close(l.closeCh)
	l.sw.Close()
	l.readCache.CleanReader()
}