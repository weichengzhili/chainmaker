package wal

type FlushStrategy int
type FileType int

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
	Fs                         FlushStrategy //刷盘策略(1同步刷盘 2手动刷盘 3延迟刷盘,可以更细化) 默认同步刷盘
	FlushValue                 int           //刷盘阈值，达到阈值再进行刷盘
	SegmentSize                uint64        //文件的大小限制 默认64M 代表不限制
	Ft                         FileType      //文件类型(1 普通文件 2 mmap) 默认1
	LogFileLimitForPurge       int           //存在日志文件限制
	LogEntryCountLimitForPurge int           //存在日志条目限制
	FilePrefix                 string
	FileExtension              string
}

type Opt func(*Options)

func WithFlushStrategy(t FlushStrategy, v int) Opt {
	return func(o *Options) {
		o.Fs = t
		o.FlushValue = v
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
