/*
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

var (
	purgeLocker = NewChansema(1)
)

type Chansema struct {
	ch chan struct{}
}

func NewChansema(n int) *Chansema {
	return &Chansema{
		ch: make(chan struct{}, n),
	}
}

func (cs *Chansema) Acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case cs.ch <- struct{}{}:
		return nil
	}
}

func (cs *Chansema) TryAcquire() bool {
	select {
	case cs.ch <- struct{}{}:
		return true
	default:
		return false
	}
}

func (cs *Chansema) Release() {
	<-cs.ch
}

type FileLock struct {
	path string
	f    *os.File
}

func NewFileLocker(path string) *FileLock {
	return &FileLock{
		path: path,
	}
}

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

func (fl *FileLock) Unlock() error {
	defer fl.f.Close()
	return syscall.Flock(int(fl.f.Fd()), syscall.LOCK_UN)
}

type segmentWaterPool struct {
	*rwlockSegmentGroup
	lastIndex uint64
}

func (swp *segmentWaterPool) fileWaterLevel() int {
	return swp.Len()
}

func (swp *segmentWaterPool) entryWaterLevel() uint64 {
	return swp.lastIndex - swp.First().Index + 1
}

type purgeGuarder struct {
	fn func()
}

func (pg *purgeGuarder) Release() {
	if pg.fn != nil {
		pg.fn()
	}
}

type purgeWorker struct {
	purgeLimit
}

func newPurgeWorker(limit purgeLimit) *purgeWorker {
	return &purgeWorker{
		purgeLimit: limit,
	}
}

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

func (pw *purgeWorker) Probe(swp segmentWaterPool) bool {
	return pw.purgeType(swp) != 0
}

func (pw *purgeWorker) Purge(swp segmentWaterPool, call func(*Segment)) error {
	var (
		boundary *Segment
		files    []string
	)
	switch pw.purgeType(swp) {
	case 1:
		boundary, files = pw.pureOverEntryLevel(swp)
	case 2:
		boundary, files = pw.pureOverFilesLevel(swp)
	}
	if boundary == nil {
		return nil
	}
	for _, fn := range files {
		os.Remove(fn)
	}
	call(boundary)
	return nil
}

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

func (pw *purgeWorker) pureOverFilesLevel(swp segmentWaterPool) (boundary *Segment, files []string) {
	threshold := swp.fileWaterLevel() - pw.keepFiles
	swp.RLock()
	swp.Traverse(func(i int, s *Segment) bool {
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

func (pw *purgeWorker) pureOverEntryLevel(swp segmentWaterPool) (boundary *Segment, files []string) {
	from := swp.lastIndex - uint64(pw.keepSoftEntries) + 1
	var (
		at int
	)
	swp.RLock()
	swp.Traverse(func(i int, s *Segment) bool {
		if s.Index > from {
			at = i
			return true
		}
		files = append(files, s.Path)
		return false
	})
	if at > 0 {
		boundary = swp.At(at - 1)
		files = files[:len(files)-1]
	}
	swp.RUnlock()
	return
}
