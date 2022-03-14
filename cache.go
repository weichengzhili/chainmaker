/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
/*
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

type ReaderCache struct {
	rw       sync.RWMutex
	readers  map[uint64]*refReader //sync.Map
	evicting int32
}

type refReader struct {
	*SegmentReader
	ref        int32
	lastAccess time.Time
}

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
func (rc *ReaderCache) evict() {
	if !atomic.CompareAndSwapInt32(&rc.evicting, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&rc.evicting, 0)
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
		if segmentID == 0 {
			return 0, 0, len(rc.readers) == 0
		}
		return time.Now().Sub(t), segmentID, len(rc.readers) == 0
	}
	evictReader := func(id uint64) {
		rc.rw.Lock()
		defer rc.rw.Unlock()
		rd, exist := rc.readers[id]
		if !exist {
			return
		}
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
		if id > 0 {
			if evictInterval-duration <= 0 {
				evictReader(id)
				continue
			}
		}
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
