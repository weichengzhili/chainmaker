/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package lws

type FlushStrategy int
type FileType int

type WriteFlag int

const (
	WF_SYNCWRITE  WriteFlag = 1                  //同步写，写系统不刷盘
	WF_TIMEDFLUSH WriteFlag = (1<<iota - 1) << 1 //定时刷盘
	WF_QUOTAFLUSH                                //日志写入数量刷盘
	WF_SYNCFLUSH                                 //同步刷盘
)

const (
	FT_NORMAL FileType = iota
	FT_MMAP
)

type Options struct {
	Wf                         WriteFlag //写日志标识
	FlushQuota                 int       //刷盘限定值
	SegmentSize                uint64    //文件的大小限制 默认64M 代表不限制
	Ft                         FileType  //文件类型(1 普通文件 2 mmap) 默认1
	MmapFileLock               bool      //文件映射的时候，是否锁定内存以提高write速度
	BufferSize                 int
	LogFileLimitForPurge       int //存在日志文件限制
	LogEntryCountLimitForPurge int //存在日志条目限制
	FilePrefix                 string
	FileExtension              string
}

type Opt func(*Options)

func WithWriteFlag(wf WriteFlag, quota int) Opt {
	return func(o *Options) {
		o.Wf = wf
		o.FlushQuota = quota
	}
}

func WithSegmentSize(s uint64) Opt {
	return func(o *Options) {
		o.SegmentSize = s
	}
}

func WithWriteFileType(ft FileType) Opt {
	return func(o *Options) {
		o.Ft = ft
	}
}

func WithFileLimitForPurge(l int) Opt {
	return func(o *Options) {
		o.LogFileLimitForPurge = l
	}
}

func WithEntryLimitForPurge(l int) Opt {
	return func(o *Options) {
		o.LogEntryCountLimitForPurge = l
	}
}

func WithFilePrex(prex string) Opt {
	return func(o *Options) {
		o.FilePrefix = prex
	}
}

func WithFileExtension(ext string) Opt {
	return func(o *Options) {
		o.FileExtension = ext
	}
}

func WithMmapFileLock() Opt {
	return func(o *Options) {
		o.MmapFileLock = true
	}
}

func WithBufferSize(s int) Opt {
	return func(o *Options) {
		o.BufferSize = s
	}
}

type PurgeOptions struct {
	mode purgeMod
	purgeLimit
}
type purgeLimit struct {
	keepFiles int
	// keepEntries     int
	keepSoftEntries int
}

type PurgeOpt func(*PurgeOptions)

func PurgeWithKeepFiles(c int) PurgeOpt {
	return func(po *PurgeOptions) {
		po.keepFiles = c
	}
}

func PurgeWithSoftEntries(c int) PurgeOpt {
	return func(po *PurgeOptions) {
		po.keepSoftEntries = c
	}
}

func PurgeWithAsync() PurgeOpt {
	return func(po *PurgeOptions) {
		po.mode = purgeModAsync
	}
}
