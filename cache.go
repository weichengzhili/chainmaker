/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package lws

import (
	"sync"
	"sync/atomic"
	"time"
)

type ReaderCache struct {
	rw      sync.RWMutex
	readers map[uint64]*refReader //sync.Map
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

func (rc *ReaderCache) PutReader(segmentID uint64, rr *refReader) {
	rc.rw.Lock()
	defer rc.rw.Unlock()
	if rc.readers == nil {
		rc.readers = make(map[uint64]*refReader)
	}
	rr.access()
	rc.readers[segmentID] = rr
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
