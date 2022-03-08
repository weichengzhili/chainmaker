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

func TestLws_Write(t *testing.T) {
	l, err := Open(testPath, WithSegmentSize(30), WithFilePrex("test_"), WithWriteFlag(WF_SYNCFLUSH, 0), WithFileLimitForPurge(3))
	require.Nil(t, err)
	data := []byte("hello world")
	err = l.Write(0, data)
	require.Nil(t, err)
	// time.Sleep(time.Second * 5)
	l.Flush()
	l.Close()
	require.Nil(t, err)
}

func TestLws_Read(t *testing.T) {
	l, err := Open(testPath, WithSegmentSize(30), WithFilePrex("test_"))
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

func TestLws_WriteFile(t *testing.T) {
	l, err := Open(testPath, WithSegmentSize(30), WithFilePrex("test_"), WithWriteFlag(WF_SYNCFLUSH, 0), WithFileLimitForPurge(3))
	require.Nil(t, err)
	data := []byte("hello world@##########################################################@@")
	err = l.WriteToFile("test_file.wal", 0, data)
	require.Nil(t, err)
	l.Close()
}

func TestLws_ReadFile(t *testing.T) {
	l, err := Open(testPath, WithSegmentSize(30), WithFilePrex("test_"), WithWriteFlag(WF_SYNCFLUSH, 0), WithFileLimitForPurge(3))
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

func TestLws_Flush(t *testing.T) {
	l, err := Open(testPath, WithSegmentSize(30), WithFilePrex("test_"),
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

func TestLws_WriteReadObj(t *testing.T) {
	err := RegisterCoder(&StudentCoder{})
	require.Nil(t, err)
	l, err := Open(testPath, WithSegmentSize(30), WithFilePrex("test_"))
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

// var (
// 	benchLws *Lws
// 	benchWal *wal.Log
// )

// func init() {
// 	l, err := Open("./lws", WithSegmentSize(1<<26), WithFilePrex("test_"), WithWriteFlag(WF_SYNCWRITE, 0))
// 	if err != nil {
// 		panic(err)
// 	}
// 	benchLws = l
// 	benchWal, err = wal.Open("./wal", &wal.Options{
// 		SegmentSize: 1 << 26,
// 		LogFormat:   wal.JSON,
// 		NoSync:      true,
// 	})
// }

// func BenchmarkLws_Write(b *testing.B) {
// 	l := benchLws
// 	data := []byte("hello world")
// 	for i := 0; i < b.N; i++ {
// 		l.Write(0, data)
// 	}
// 	l.Flush()
// }

// func BenchmarkWal_Write(b *testing.B) {
// 	l := benchWal
// 	last, _ := l.LastIndex()
// 	data := []byte("hello world")
// 	for i := 0; i < b.N; i++ {
// 		last++
// 		l.Write(last, data)
// 	}
// 	l.Sync()
// }
