/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package lws

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	evictInterval = 3 * time.Minute
)

//ReaderCache reader缓存器
type ReaderCache struct {
	rw       sync.RWMutex
	readers  map[uint64]*refReader
	evicting int32 //true正在检测并淘汰过期reader
}

//refReader 带有引用计数和最近访问事件的reader
type refReader struct {
	*SegmentReader
	ref        int32
	lastAccess time.Time
}

//GetReader 通过段ID获取reader，不存在则返回nil
func (rc *ReaderCache) GetReader(segmentID uint64) *refReader {
	rc.rw.RLock()
	v, ok := rc.readers[segmentID]
	rc.rw.RUnlock()
	if !ok {
		return nil
	}
	v.access()
	return v
}

//GetAndNewReader 通过段ID获取reader，如果reader不存在，则通过new函数创建并添加到缓存中
func (rc *ReaderCache) GetAndNewReader(segmentID uint64, new func() (*refReader, error)) (*refReader, error) {
	rc.rw.RLock()
	v, ok := rc.readers[segmentID]
	rc.rw.RUnlock()
	if !ok {
		if new == nil {
			return nil, errors.New("new func is nil")
		}
		var (
			err error
		)
		v, err = new()
		if err != nil {
			return nil, err
		}
		v.access()
		rc.rw.Lock()
		rc.put(segmentID, v)
		rc.rw.Unlock()
	}
	return v, nil
}

func (rc *ReaderCache) put(segmentID uint64, rr *refReader) {
	if rc.readers == nil {
		rc.readers = make(map[uint64]*refReader)
	}
	rc.readers[segmentID] = rr
	if atomic.LoadInt32(&rc.evicting) == 0 {
		go rc.evict()
	}
}

func (rc *ReaderCache) PutReader(segmentID uint64, rr *refReader) {
	rc.rw.Lock()
	defer rc.rw.Unlock()
	rr.access()
	rc.put(segmentID, rr)

}

func (rc *ReaderCache) DeleteReader(segmentID uint64) *refReader {
	rc.rw.Lock()
	defer rc.rw.Unlock()
	if v, ok := rc.readers[segmentID]; ok {
		delete(rc.readers, segmentID)
		return v
	}
	return nil
}

func (rc *ReaderCache) CleanReader() {
	rc.rw.Lock()
	defer rc.rw.Unlock()
	for id, v := range rc.readers {
		v.Close()
		delete(rc.readers, id)
	}
}

//evict 对缓存中的reader进行检测和清除
func (rc *ReaderCache) evict() {
	if !atomic.CompareAndSwapInt32(&rc.evicting, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&rc.evicting, 0)
	//nextTime，获取最近要淘汰的reader的信息（剩余时间，对应的ID，及缓存是否为空)
	nextTime := func() (time.Duration, uint64, bool) {
		var (
			segmentID uint64
			t         = time.Now()
		)

		rc.rw.Lock()
		defer rc.rw.Unlock()
		for id, v := range rc.readers {
			if v.ref == 0 && t.After(v.lastAccess) {
				t = v.lastAccess
				segmentID = id
			}
		}
		if segmentID == 0 { //如果为找到ID，则代表缓存目前为空，驱逐器可以依据此stop
			return 0, 0, len(rc.readers) == 0
		}
		return time.Now().Sub(t), segmentID, len(rc.readers) == 0
	}
	//通过id驱逐对应的reader
	evictReader := func(id uint64) {
		rc.rw.Lock()
		defer rc.rw.Unlock()
		rd, exist := rc.readers[id]
		if !exist {
			return
		}
		//检测reader是否超时，如若超时，则进行删除
		if rd.ref == 0 && time.Now().Sub(rd.lastAccess) >= evictInterval {
			delete(rc.readers, id)
		}
	}
	timer := time.NewTimer(evictInterval)
	for {
		duration, id, stop := nextTime()
		if stop {
			return
		}
		if id > 0 { //如果找到reader， 并且已经超时，则直接清除
			if evictInterval-duration <= 0 {
				evictReader(id)
				continue
			}
		}
		//延迟到reader过期时间
		timer.Reset(evictInterval - duration)
		<-timer.C
	}
}

func (rr *refReader) Obtain() {
	atomic.AddInt32(&rr.ref, 1)
}

func (rr *refReader) Release() {
	atomic.AddInt32(&rr.ref, -1)
}

func (rr *refReader) access() {
	rr.lastAccess = time.Now()
}

func (rr *refReader) ReadLogByIndex(index uint64) (*LogEntry, error) {
	rr.access()
	return rr.SegmentReader.ReadLogByIndex(index)
}
