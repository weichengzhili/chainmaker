/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package lws

import (
	"context"
	"fmt"
	"os"
	"syscall"
)

type writeNoticeType int8

const (
	newFile writeNoticeType = 1 << iota
	newLog
)

var (
	purgeLocker = NewChansema(1)
)

//chan实现的信号量
type Chansema struct {
	ch chan struct{}
}

func NewChansema(n int) *Chansema {
	return &Chansema{
		ch: make(chan struct{}, n),
	}
}

//Acquire block acquiring semaphore until acquire successfully or context cancel/timeout
func (cs *Chansema) Acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case cs.ch <- struct{}{}:
		return nil
	}
}

//TryAcquire  non-block acquiring semaphore，if successfully return true, otherwise return false
func (cs *Chansema) TryAcquire() bool {
	select {
	case cs.ch <- struct{}{}:
		return true
	default:
		return false
	}
}

//Release release the semaphore has acquired
func (cs *Chansema) Release() {
	<-cs.ch
}

//FileLock Used to exclusively lock a file
type FileLock struct {
	path string
	f    *os.File
}

func NewFileLocker(path string) *FileLock {
	return &FileLock{
		path: path,
	}
}

//Lock non-block adding an exclusive lock to a file, if successfully return nil, otherwise return a error
func (fl *FileLock) Lock() error {
	f, err := os.Open(fl.path)
	if err != nil {
		return err
	}
	fl.f = f
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return fmt.Errorf("cannot lock file %s - %s", fl.path, err)
	}
	return nil
}

//Unlock release the exclusive lock
func (fl *FileLock) Unlock() error {
	defer fl.f.Close()
	return syscall.Flock(int(fl.f.Fd()), syscall.LOCK_UN)
}

//segmentWaterPool all file segment like the water in a some water pool, as the number of file segments increases, the water level increases
//now has two kind of water level, one is files level, anther is log entey level
type segmentWaterPool struct {
	*rwlockSegmentGroup
	lastIndex uint64
}

//fileWaterLevel return the current level of file segment
func (swp *segmentWaterPool) fileWaterLevel() int {
	return swp.Len()
}

//entryWaterLevel return the current level of log entries
func (swp *segmentWaterPool) entryWaterLevel() uint64 {
	return swp.lastIndex - swp.First().Index + 1
}

//purgeGuarder used to generate a guarder to guard the locked resources; fn is called to release the locked resources
type purgeGuarder struct {
	fn func()
}

//Release call fn to release the locked resources
func (pg *purgeGuarder) Release() {
	if pg.fn != nil {
		pg.fn()
	}
}

// purgeWorker represents a cleanup process who knows the clean up standard
type purgeWorker struct {
	purgeLimit
}

func newPurgeWorker(limit purgeLimit) *purgeWorker {
	return &purgeWorker{
		purgeLimit: limit,
	}
}

//Guard locks the resources
func (pw *purgeWorker) Guard() *purgeGuarder {
	if purgeLocker.TryAcquire() {
		return &purgeGuarder{
			fn: func() {
				purgeLocker.Release()
			},
		}
	}
	return nil
}

//Probe detect if cleaning is required
func (pw *purgeWorker) Probe(swp segmentWaterPool) bool {
	return pw.purgeType(swp) != 0
}

func (pw *purgeWorker) Purge(swp segmentWaterPool, call func(*Segment)) error {
	var (
		boundary *Segment
		files    []string
	)
	switch pw.purgeType(swp) {
	case 1: //type 1: log entry limit reached
		boundary, files = pw.pureOverEntryLevel(swp)
	case 2: //type 2: file limit reached
		boundary, files = pw.pureOverFilesLevel(swp)
	}
	//boundary no pure worker need to do
	if boundary == nil {
		return nil
	}
	//delete files
	for _, fn := range files {
		os.Remove(fn)
	}
	//call: invoke upper-level processing logic
	call(boundary)
	return nil
}

//purgeType return the pure type, 0: no purge worker, 1 log entry limit reached, 2 file limit reached
func (pw *purgeWorker) purgeType(swp segmentWaterPool) int {
	trigger := pw.keepSoftEntries > 0 && swp.entryWaterLevel() > uint64(pw.keepSoftEntries)
	if trigger {
		return 1
	}
	trigger = pw.keepFiles > 0 && swp.fileWaterLevel() > pw.keepFiles
	if trigger {
		return 2
	}
	return 0
}

//pureOverFilesLevel calculate boundary and filenames to clean based on file limits
func (pw *purgeWorker) pureOverFilesLevel(swp segmentWaterPool) (boundary *Segment, files []string) {
	//maximum segment index to clean
	threshold := swp.fileWaterLevel() - pw.keepFiles
	swp.RLock()
	swp.ForEach(func(i int, s *Segment) bool {
		if i < threshold {
			files = append(files, s.Path)
			return false
		}
		boundary = s
		return true
	})
	swp.RUnlock()
	return
}

//pureOverEntryLevel calculate boundary and filenames to clean based on log entry limits
func (pw *purgeWorker) pureOverEntryLevel(swp segmentWaterPool) (boundary *Segment, files []string) {
	//minimum log entry index to keep
	from := swp.lastIndex - uint64(pw.keepSoftEntries) + 1
	var (
		at int
	)
	swp.RLock()
	//find the file name of segments whose index less than from, and find the first segment whose index value is greater than from
	swp.ForEach(func(i int, s *Segment) bool {
		if s.Index > from {
			at = i
			return true
		}
		files = append(files, s.Path)
		return false
	})
	//if at > 0, the index value is in the previous segment of at, so boundary is at-1, files need remove the last element
	if at > 0 {
		boundary = swp.At(at - 1)
		files = files[:len(files)-1]
	}
	swp.RUnlock()
	return
}
