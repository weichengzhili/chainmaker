/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package file

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	value1024 = "5f7575632450456b625c49725d336a7e2158245a7c652b6e2252495e453254353f5b453c5e50" +
		"5d6d734c732c2a40763d4c294168347c4638535a2f7730444c215a5333534d745c353733554f5c3d6d545" +
		"e474d286e4e2849554928433379315d366726477d72454628362a6a3b442c716c6f2a544e63565c4f2827" +
		"443f662527696561405d296f56703727474a2b657d322e316b2d57443971655724555d3c4129783c6a292" +
		"2405555665e5c644f2769217176634c613c4358265634502a546350754c25384e2c786c652f5775623332" +
		"52292a48202e685e5b382d353f7a7c6d22617c692e774d69366a646b696a51294c3162334b65425067327" +
		"d74307c246221523a6a697356393c66345e687e7b763362357851764f552159695f2f7876664e54657c54" +
		"24563844492c664a4021436a6d70222b795670534370502032623b434f3a286f2f35453f2d517a50666d6" +
		"c4c29224e4673655a2c4f2f57637d43756a2e756d7d236e5c4674326d2c2c2b3e51734362246a7d697e2d" +
		"46733d5d337a376746443e6122217225727024205c2f7825687d5a52332328606963293857393b2841396" +
		"b225f73652f533f302e7359522d2a634b2e6f2b236e7a66432c6d6d7851565e385a494146433f3332573d" +
		"5225542c5c29525861703c2956215e4e24514e6b32233e2a3b3b5d406e2c6b5525426135683d563d6a5e7335786e757e47"

	valueMB  = ""
	valueNMB = ""
)

func init() {
	for i := 0; i < 1024; i++ {
		valueMB = valueMB + value1024
	}

	for i := 0; i < 30; i++ {
		valueNMB = valueNMB + valueMB
	}
}

func TestMmapWrite(t *testing.T) {
	fileSize := 1 << 30
	map_size := 1 << 30
	f, err := NewMmapFile("./test_mmap.wal", map_size)
	err = f.Truncate(int64(fileSize))
	require.Nil(t, err)
	//data := []byte("hello world@@")
	data := []byte(valueNMB)
	for i := 0; i < map_size/len(data); i++ {
		f.Write(data)
		f.Sync()
	}
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

func TestFileWrite(t *testing.T) {
	fileSize := 1 << 30
	f, err := NewFile("./test_mmap2.wal")
	require.Nil(t, err)
	err = f.Truncate(int64(fileSize))
	require.Nil(t, err)
	//data := []byte("hello world@@")
	data := []byte(valueNMB)
	for i := 0; i < fileSize/len(data); i++ {
		f.Write(data)
		f.Sync()
	}
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

func mmapT(i int) {
	fileSize := 1 << 30
	map_size := 1 << 30
	f, err := NewMmapFile(fmt.Sprintf("./test_mmap%d.wal", i), map_size)
	if err != nil {
		panic(err)
	}
	err = f.Truncate(int64(fileSize))
	//data := []byte("hello world@@")
	data := []byte(valueNMB)
	for i := 0; i < map_size/len(data); i++ {
		f.Write(data)
		f.Sync()
	}
	//f.Sync()
	f.Close()
}

func mmapF(i int) {
	fileSize := 1 << 30
	map_size := 1 << 30
	f, err := NewFile(fmt.Sprintf("./test_normal%d.wal", i))
	if err != nil {
		panic(err)
	}
	err = f.Truncate(int64(fileSize))
	//data := []byte("hello world@@")
	data := []byte(valueNMB)
	for index := 0; index < map_size/len(data); index++ {
		f.Write(data)
		f.Sync()
	}

	f.Close()
}

func BenchmarkMmap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		mmapT(i)
	}
}

func BenchmarkFile(b *testing.B) {
	for i := 0; i < b.N; i++ {
		mmapF(i)
	}
}
