/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package lws

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	testPath = "./log"
)

func TestWrite(t *testing.T) {
	l, err := Open(testPath, WithSegmentSize(30), WithFilePrex("test_"), WithFlushStrategy(FlushStrategyManual, 0), WithFileLimitForPurge(3))
	require.Nil(t, err)
	data := []byte("hello world")
	err = l.Write(0, data)
	require.Nil(t, err)
	l.Flush()
	l.Close()
	require.Nil(t, err)
}

func TestRead(t *testing.T) {
	l, err := Open(testPath, WithSegmentSize(30), WithWriteFileType(FileTypeMmap), WithFilePrex("test_"), WithFlushStrategy(FlushStrategySync, 0), WithFileLimitForPurge(3))
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
	l.Close()
}

func TestWriteFile(t *testing.T) {
	l, err := Open(testPath, WithSegmentSize(30), WithWriteFileType(FileTypeMmap), WithFilePrex("test_"), WithFlushStrategy(FlushStrategySync, 0), WithFileLimitForPurge(3))
	require.Nil(t, err)
	data := []byte("hello world@##########################################################@@")
	err = l.WriteToFile("test_file.wal", 0, data)
	require.Nil(t, err)
	l.Close()
}

func TestReadFile(t *testing.T) {
	l, err := Open(testPath, WithSegmentSize(30), WithWriteFileType(FileTypeMmap), WithFilePrex("test_"), WithFlushStrategy(FlushStrategySync, 0), WithFileLimitForPurge(3))
	require.Nil(t, err)
	it, err := l.ReadFromFile("test_file.wal")
	require.Nil(t, err)
	for it.HasNext() {
		data, err := it.Next().Get()
		if err != nil {
			t.Log("err:", err)
		} else {
			t.Log(string(data))
		}
	}
	t.Log("over")
	l.Close()
}

func TestPurgeStrategy(t *testing.T) {
	l, err := Open(testPath, WithSegmentSize(30), WithWriteFileType(FileTypeMmap), WithFilePrex("test_"),
		WithFileLimitForPurge(6), WithEntryLimitForPurge(10))
	require.Nil(t, err)
	for i := 0; i < 5; i++ {
		data := []byte("hello world")
		err = l.Write(0, data)
		time.Sleep(2 * time.Second)
		require.Nil(t, err)
	}
	l.Flush()
	l.Close()
}

type Student struct {
	Age   int
	Name  string
	Grade int
	Class int
}

type StudentCoder struct {
}

func (sc *StudentCoder) Type() int8 {
	return 1
}

func (sc *StudentCoder) Encode(s interface{}) ([]byte, error) {
	return json.Marshal(s)
}
func (sc *StudentCoder) Decode(data []byte) (interface{}, error) {
	var (
		s Student
	)
	err := json.Unmarshal(data, &s)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func TestWriteReadObj(t *testing.T) {
	err := RegisterCoder(&StudentCoder{})
	require.Nil(t, err)
	l, err := Open(testPath, WithSegmentSize(30), WithWriteFileType(FileTypeMmap), WithFilePrex("test_"))
	require.Nil(t, err)
	s := Student{
		Name:  "lucy",
		Age:   10,
		Grade: 3,
		Class: 1,
	}
	for i := 0; i < 5; i++ {
		s.Age++
		err = l.Write(1, s)
		require.Nil(t, err)
	}
	l.Flush()
	it := l.NewLogIterator()
	it.SkipToLast()
	for i := 0; it.HasPre() && i < 5; i++ {
		obj, err := it.Previous().GetObj()
		if err != nil {
			t.Log("err:", err)
		} else {
			t.Log(obj)
		}
	}
	l.Close()
}

func BenchmarkWriteLws(b *testing.B) {
	l, err := Open("./lws", WithSegmentSize(1<<26), WithWriteFileType(FileTypeNormal), WithFilePrex("test_"), WithFlushStrategy(FlushStrategyManual, 0))
	if err != nil {
		panic(err)
	}
	data := []byte("hello world")
	for i := 0; i < b.N; i++ {
		l.Write(0, data)
	}
	l.Flush()
}

// func BenchmarkWriteWal(b *testing.B) {
// 	l, err := wal.Open("./wal", &wal.Options{
// 		SegmentSize: 1 << 26,
// 		LogFormat:   wal.JSON,
// 		NoSync:      true,
// 	})
// 	if err != nil {
// 		panic(err)
// 	}
// 	last, _ := l.LastIndex()
// 	data := []byte("hello world")
// 	for i := 0; i < b.N; i++ {
// 		last++
// 		l.Write(last, data)
// 	}
// }
