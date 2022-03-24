/*
Copyright (C) BABEC. All rights reserved.
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

	"chainmaker.org/chainmaker/lws/dsl"
)

var (
	timeDelay   = 1000 //ms
	defaultOpts = Options{
		SegmentSize:   1 << 26,
		Ft:            FT_MMAP,
		FileExtension: "wal",
		Wf:            WF_TIMEDFLUSH,
		FlushQuota:    timeDelay,
		BufferSize:    -1, //-1无配置
	}

	fileReg             = `%s\d{5}_\d+\.%s`
	ErrPurgeWorkExisted = errors.New("purge work has been performed")
	ErrPurgeNotReached  = errors.New("purge threshold not reached")

	InitID    = 1
	InitIndex = 1
)

type Lws struct {
	mu               sync.Mutex
	path             string //base path of log files
	opts             Options
	sw               *SegmentWriter //lws writes log data through it
	currentSegmentID uint64         //the id of latest segment
	firstIndex       uint64
	lastIndex        uint64 //the last index of log entry has been writen
	segments         rwlockSegmentGroup
	readCache        ReaderCache //cache data wait to be readed
	cond             *sync.Cond
	readCount        int                  //record the count of reading the wal file
	writeNoticeCh    chan writeNoticeType //notice purge go routine that a new log/a new file has been writed
	closeCh          chan struct{}
	coders           *coderMap
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
	sl, err := dsl.Parse(path)
	if err != nil {
		return nil, err
	}
	return OpenWithDSL(sl, opt...)
}

/*
 @title: OpenWithDSL
 @description: open a new lws instance with struct dsl
 @param {*dsl.DSL} 数据存储定位结构，其中包括协议及路径
 @param {...Opt} opt 打开日志写入系统的参数配置
 @return {*Lws} 日志写入系统实例句柄
 @return {error} 错误信息
*/
func OpenWithDSL(sl *dsl.DSL, opt ...Opt) (*Lws, error) {
	if !dsl.IsSupportedForSchema(sl.Schema) {
		return nil, dsl.ErrNotSupport
	}

	lws := &Lws{
		path:    sl.Path,
		opts:    defaultOpts,
		cond:    sync.NewCond(&sync.Mutex{}),
		closeCh: make(chan struct{}),
		coders:  newCoderMap(),
	}
	if err := lws.open(opt...); err != nil {
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
	//构建所有wal文件的segment信息
	if err = l.buildSegments(); err != nil {
		return err
	}
	//如若没有文件，则初始化起始segment
	if l.segments.Len() == 0 {
		l.currentSegmentID = 1
		l.lastIndex = 0
		l.segments.Append(&Segment{
			ID:    uint64(InitID),
			Index: uint64(InitIndex),
			Path:  filepath.Join(l.path, l.segmentName(1, 1)),
		})
	}
	currentSegment := l.segments.Last()
	l.currentSegmentID = currentSegment.ID
	//根据最新文件的segment信息创建SegmentWriter用于写wal日志
	l.sw, err = NewSegmentWriter(currentSegment, WriterOptions{
		SegmentSize: l.opts.SegmentSize,
		Ft:          l.opts.Ft,
		Wf:          l.opts.Wf,
		Fv:          l.opts.FlushQuota,
		MapLock:     l.opts.MmapFileLock,
		BufferSize:  l.opts.BufferSize,
	})
	if err != nil {
		return err
	}
	//计算日志条目的最新索引
	l.lastIndex = currentSegment.Index + uint64(l.sw.EntryCount()) - 1
	//计算日志条目的起始索引
	l.firstIndex = l.segments.First().Index

	return nil
}

func (l *Lws) buildSegments() error {
	if err := os.MkdirAll(l.path, 0777); err != nil {
		return err
	}
	//根据wal命名规则匹配文件夹下所有wal文件
	names, err := l.matchFiles()
	if err != nil {
		return err
	}
	sort.Strings(names)
	l.segments.Resize(len(names))
	//为每个文件生成segment信息
	for i, name := range names {
		fullPath := path.Join(l.path, name)
		id, index, err := l.parseSegmentName(name)
		if err != nil {
			return err
		}
		l.segments.Assign(i, &Segment{
			ID:    id,
			Index: index,
			Path:  fullPath,
			Size:  l.fileSize(fullPath), //填充每个文件的大小，在读取文件时缓存使用
		})
	}
	return nil
}

//根据wal命名规则匹配文件夹下所有wal文件
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

func (l *Lws) fileSize(file string) int64 {
	s, err := os.Stat(file)
	if err != nil {
		return 0
	}
	return s.Size()
}

func (l *Lws) rollover() error {
	l.currentSegmentID++
	s := &Segment{
		ID:    l.currentSegmentID,
		Index: l.lastIndex + 1,
		Path:  filepath.Join(l.path, l.segmentName(l.currentSegmentID, l.lastIndex+1)),
	}
	l.segments.Lock()
	l.segments.Append(s)
	l.segments.Unlock()
	return l.sw.Replace(s)
}

//segmentName生成wal文件名
func (l *Lws) segmentName(id, idx uint64) string {
	return fmt.Sprintf("%s%05d_%d.%s", l.opts.FilePrefix, id, idx, l.opts.FileExtension)
}

//parseSegmentName 通过wal文件名解析出ID、index信息
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
	_, err := l.write(typ, obj)
	return err
}

/*
 @title: WriteBytes
 @description: 将字节流写入文件
 @param {[]byte} data  数据
 @return {error} 成功返回entry的索引值&nil, 失败返回0&err
*/
func (l *Lws) WriteBytes(data []byte) (uint64, error) {
	return l.write(0, data)
}

/*
 @title: WriteRetIndex
 @description: 将obj对象写入文件
 @param {int8} typ 写入的数据类型
 @param {interface{}} obj  数据
 @return {error} 成功返回entry的索引值&nil, 失败返回0&err
*/
func (l *Lws) WriteRetIndex(typ int8, obj interface{}) (uint64, error) {
	return l.write(typ, obj)
}

func (l *Lws) write(typ int8, obj interface{}) (uint64, error) {
	t, data, err := l.encodeObj(typ, obj) //序列化obj对象
	if err != nil {
		return 0, nil
	}
	var (
		writeNotice writeNoticeType //写入通知信息，用于通知purgework有新日志写入
	)
	l.mu.Lock()
	defer l.mu.Unlock()
	//判断是否需要分割文件
	if l.opts.SegmentSize > 0 && l.sw.Size() > l.opts.SegmentSize {
		writeNotice |= newFile //如果创建新文件则通知信息中加入newFile类型
		if err = l.rollover(); err != nil {
			return 0, err
		}
	}
	if _, err = l.sw.Write(t, data); err != nil {
		return 0, err
	}
	writeNotice |= newLog //写log成功则在通知信息中加入newLog类型
	l.lastIndex++
	l.writeNotice(writeNotice)
	return l.lastIndex, nil
}

func (l *Lws) encodeObj(t int8, obj interface{}) (int8, []byte, error) {
	data, ok := obj.([]byte)
	if !ok {
		coder, err := l.coders.GetCoder(t)
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
	//读请求+1，组织后台清理程序清理文件
	l.readRequest()
	it := newEntryIterator(
		&walContainer{
			wal:   l,
			first: l.firstIndex,
			last:  l.lastIndex,
		},
	)
	// runtime.SetFinalizer(it, func(it *EntryIterator) {
	// 	it.Release()
	// })
	return it
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
func (l *Lws) Purge(opt ...PurgeOpt) error {
	opts := PurgeOptions{}
	for _, o := range opt {
		o(&opts)
	}
	switch opts.mode {
	case purgeModAsync:
		go l.purge(opts.purgeLimit)
	case purgeModSync:
		return l.purge(opts.purgeLimit)
	}
	return nil
}

func (l *Lws) purge(limit purgeLimit) error {
	//根据限额指标（文件保留数&日志条目保留数)，创建PurgeWorker
	pworker := newPurgeWorker(limit)
	pool := segmentWaterPool{
		rwlockSegmentGroup: &l.segments,
		lastIndex:          l.lastIndex,
	}
	//探测是否需要进行清理工作，以减少后续的资源竞争
	if !pworker.Probe(pool) {
		return nil
	}
	//清理加锁，如果加锁失败，说明目前有清理程序正在工作
	gurder := pworker.Guard()
	if gurder == nil {
		return ErrPurgeWorkExisted
	}
	defer gurder.Release()
	//等待wal迭代器都释放掉才可以进行清理工作
	l.cond.L.Lock()
	for l.readCount > 0 {
		l.cond.Wait()
	}
	//purgeworker会检测到要清理到的边界文件Segment，lws根据边界文件的信息进行本身状态重置
	callBack := func(boundary *Segment) {
		if boundary != nil {
			l.firstIndex = boundary.Index
			l.cond.L.Unlock()
			l.segments.Lock()
			defer l.segments.Unlock()
			var at int
			l.segments.ForEach(func(i int, s *Segment) bool {
				if s.ID < boundary.ID {
					if rd := l.readCache.DeleteReader(s.ID); rd != nil {
						rd.Close()
					}
					return false
				}
				at = i - 1
				return true
			})
			if at >= 0 {
				_, l.segments.SegmentGroup = l.segments.Split(at)
			}
		}
	}
	return pworker.Purge(segmentWaterPool{
		rwlockSegmentGroup: &l.segments,
		lastIndex:          l.lastIndex,
	}, callBack)
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
	//检测要写的文件是否与wal命名规则相同，如果相同则阻值
	reg, err := regexp.Compile(fmt.Sprintf(fileReg, l.opts.FilePrefix, l.opts.FileExtension))
	if err != nil {
		return err
	}
	if reg.Match([]byte(file)) {
		return errors.New("the file name is invalid: filename should circumvent the wal filename rules")
	}
	t, data, err := l.encodeObj(typ, obj)
	if err != nil {
		return err
	}
	//生成文件SegmentWriter对文件进行写入操作，因为一般此操作是一次性操作，故使用了普通文件无缓存的模式
	sw, err := NewSegmentWriter(&Segment{
		Path: path.Join(l.path, file),
	}, WriterOptions{
		Ft: FT_NORMAL,
		Wf: WF_SYNCFLUSH,
	})
	if err != nil {
		return err
	}
	_, err = sw.Write(t, data)
	return err
}

func (l *Lws) ReadFromFile(file string) (*EntryIterator, error) {
	path := path.Join(l.path, file)
	finfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	sr, err := NewSegmentReader(&Segment{
		Path:  path,
		Index: 1,
		Size:  finfo.Size(),
	}, FT_NORMAL)
	if err != nil {
		return nil, err
	}
	return newEntryIterator(
		&fileContainer{
			SegmentReader: sr,
			coders:        l.coders,
		}), nil
}

func (l *Lws) findReaderByIndex(idx uint64) (*refReader, error) {
	//根据index获取segment信息，如若为nil，说明index不在范围内
	s := l.findSegmentByIndex(idx)
	if s == nil {
		return nil, errors.New("idx out of range")
	}
	//从readCache中获取reader，如果不存在则通过传入的函数生成
	return l.readCache.GetAndNewReader(s.ID, func() (*refReader, error) {
		sr, err := NewSegmentReader(s, l.opts.Ft)
		if err != nil {
			return nil, err
		}
		return &refReader{
			SegmentReader: sr,
		}, nil
	})
}

func (l *Lws) findSegmentByIndex(idx uint64) *Segment {
	l.segments.RLock()
	defer l.segments.RUnlock()
	return l.segments.FindAt(idx)
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
			fileCount = l.segments.Len()
			entryCount = l.lastIndex - l.firstIndex + 1
		}
	)
	reassign() //初时化文件数目&日志条目数信息
	for {
		select {
		case t := <-l.writeNoticeCh: //监听到写入通知
			if t&newLog != 0 {
				entryCount++
			}
			if t&newFile != 0 {
				fileCount++
			}
			//判断是否需要进行文件清理
			if (l.opts.LogEntryCountLimitForPurge > 0 && entryCount > uint64(l.opts.LogEntryCountLimitForPurge)) ||
				(l.opts.LogFileLimitForPurge > 0 && fileCount > l.opts.LogFileLimitForPurge) {
				l.purge(purgeLimit{
					keepFiles:       l.opts.LogFileLimitForPurge,
					keepSoftEntries: l.opts.LogEntryCountLimitForPurge,
				})
				reassign() //重置文件数目&日志条目数信息
			}
		case <-l.closeCh:
			return
		}
	}
}

func (l *Lws) RegisterCoder(c Coder) error {
	return l.coders.RegisterCoder(c)
}

func (l *Lws) UnregisterCoder(t int8) error {
	return l.coders.UnregisterCoder(t)
}

func (l *Lws) Close() {
	l.sw.Close()
	l.readCache.CleanReader()
	close(l.closeCh)
}
