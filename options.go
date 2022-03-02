/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package lws

type FlushStrategy int
type FileType int

type WriteFlag int

const (
	WF_SYNCWRITE WriteFlag = 1 //同步写，写系统不刷盘

	WF_TIMEDFLUSH WriteFlag = (1<<iota - 1) << 1 //定时刷盘
	WF_QUOTAFLUSH                                //日志写入数量刷盘
	WF_SYNCFLUSH                                 //同步刷盘
)

const (
	FlushStrategySync          FlushStrategy = 1 << iota //同步刷盘
	FlushStrategyManual                                  //手动刷盘
	FlushStrategyTimeDelay                               //按照时间延迟刷盘
	FlushStrategyCapDelay                                //写入特定容量再刷盘
	FlushStrategyQuantityDelay                           //依据写入日志条目数量刷盘
)

const (
	FileTypeNormal FileType = iota
	FileTypeMmap
)

type Options struct {
	Wf                         WriteFlag
	FlushQuota                 int
	SegmentSize                uint64   //文件的大小限制 默认64M 代表不限制
	Ft                         FileType //文件类型(1 普通文件 2 mmap) 默认1
	LogFileLimitForPurge       int      //存在日志文件限制
	LogEntryCountLimitForPurge int      //存在日志条目限制
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
