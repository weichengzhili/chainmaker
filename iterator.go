package wal

type EntryContainer struct {
	wal   *Wal
	first int //first 第一个log entry的位置
	last  int //最新log entry的位置
	// reader *readerRef  //读取日志时候的预读缓存
}

type EntryIterator struct {
	index     int             //迭代器当前的位置
	container *EntryContainer //日志容器
}

type EntryElemnet struct {
	index     int
	container *EntryContainer
	// data      Entry
}
