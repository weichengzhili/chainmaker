package lws

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	l, err := Open("/root/go/src/chainmaker.org/lws/log", WithSegmentSize(30), WithWriteFileType(FileTypeMmap), WithFilePrex("test_"), WithFlushStrategy(FlushStrategySync, 0), WithFileLimitForPurge(3))
	require.Nil(t, err)
	data := []byte("hello world")
	err = l.Write(0, data)
	require.Nil(t, err)
	err = l.Purge(PurgeModSync)
	require.Nil(t, err)
}

func TestRead(t *testing.T) {
	l, err := Open("/root/go/src/chainmaker.org/lws/log", WithSegmentSize(30), WithWriteFileType(FileTypeMmap), WithFilePrex("test_"), WithFlushStrategy(FlushStrategySync, 0), WithFileLimitForPurge(3))
	require.Nil(t, err)
	it := l.NewLogIterator()
	for it.HasNext() {
		data, err := it.Next().Get()
		if err != nil {
			t.Log("err:", err)
		} else {
			t.Log(string(data))
		}
	}
}
