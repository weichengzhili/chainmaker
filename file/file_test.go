/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package file

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMmapWrite(t *testing.T) {
	fileSize := 1 << 12
	map_size := 1 << 16
	f, err := NewMmapFile("./test_mmap.wal", map_size)
	err = f.Truncate(int64(fileSize))
	require.Nil(t, err)
	data := []byte("hello world@@")
	for i := 0; i < map_size/len(data); i++ {
		f.Write(data)
	}
	f.Sync()
	f.Close()
}

func TestMmapRead(t *testing.T) {
	fileSize := 1 << 12
	map_size := 1 << 16
	f, err := NewMmapFile("./test_mmap.wal", map_size)
	require.Nil(t, err)
	err = f.Truncate(int64(fileSize))
	require.Nil(t, err)
	data := make([]byte, 1<<12)
	for {
		n, err := f.Read(data)
		if err != nil {
			if err == io.EOF {
				t.Logf("readN: %d, data:%s", n, data)
				err = nil
			}
			require.Nil(t, err)
			break
		}

		t.Logf("readN: %d, data:%s", n, data)
	}
	err = f.Close()
	require.Nil(t, err)
}

func TestMmapWriteAt(t *testing.T) {
	map_size := 1 << 16
	f, err := NewMmapFile("./test_mmap.wal", map_size)
	require.Nil(t, err)
	data := []byte("hello world@@")
	_, err = f.WriteAt(data, 0)
	require.Nil(t, err)
	f.Sync()
	f.Close()
}

func TestMmapReadAt(t *testing.T) {
	map_size := 1 << 16
	f, err := NewMmapFile("./test_mmap.wal", map_size)
	require.Nil(t, err)
	data := make([]byte, 10)
	_, err = f.ReadAt(data, 2)
	require.Nil(t, err)
	t.Log(string(data))
	err = f.Close()
	require.Nil(t, err)
}

func TestFileWrite(t *testing.T) {
	// fileSize := 1 << 12
	f, err := NewFile("./test_mmap2.wal")
	require.Nil(t, err)
	// err = f.Truncate(int64(fileSize))
	require.Nil(t, err)
	data := []byte("hello world@@")
	// for i := 0; i < fileSize/len(data); i++ {
	// 	f.Write(data)
	// }
	_, err = f.WriteAt(data, -1)
	require.Nil(t, err)
	f.Sync()
	f.Close()
}

func TestFileRead(t *testing.T) {
	fileSize := 1 << 12
	f, err := NewFile("./test_mmap2.wal")
	require.Nil(t, err)
	err = f.Truncate(int64(fileSize))
	require.Nil(t, err)
	data := make([]byte, 1<<12)
	for {
		n, err := f.Read(data)
		if err != nil {
			if err == io.EOF {
				t.Logf("readN: %d, data:%s", n, data[:n])
				err = nil
			}
			require.Nil(t, err)
			break
		}

		t.Logf("readN: %d, data:%s", n, data[:n])
	}
	err = f.Close()
	require.Nil(t, err)
}

func TestReadAt(t *testing.T) {
	f, err := NewFile("./test_mmap2.wal")
	require.Nil(t, err)
	data := make([]byte, 1<<12)
	_, err = f.ReadAt(data, -1)
	require.Nil(t, err)
	f.Close()
}
