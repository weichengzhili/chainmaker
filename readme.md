<!--
 * @Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.: 
 * @SPDX-License-Identifier: Apache-2.0: 
-->
## lws(log write system)日志写入系统
### 1. 介绍(什么是lws)
lws，日志写入系统，用于日志的写入和迭代读取
### 2. 为什么要自研lws
现在系统中用的wal日志系统，要么结构比较死板，要么写入性能相对不尽人意，针对这个情况并结合项目的使用场景，设计并开发了我们自己的lws高性能日志系统
### 3. lws的特点
* 支持对文件写入的大小设定和日志分割
* 支持对特定文件的写入
* 支持对所有日志的遍历读取，以及指定特定文件读取
* 支持对日志数据的定制化序列化和反序列化
* 针对不同场景，支持不同的日志刷盘策略（同步刷盘，手动刷盘，延时刷盘，写入日志累计条数刷盘，写入日志累计大小刷盘）
* 日志文件的自动清理机制
* 底层抽象性多种文件使用方式，包括不限于普通文件方式，内存映射方式（推荐/默认），socket远程发送方式...
  
### 4. lws使用方式
1. lws可选参数如下：
```
type Options struct {
	Fs                         FlushStrategy //刷盘策略(1同步刷盘 2手动刷盘 3时间延迟刷盘 4写入日志累计大小刷盘 5写入日志累计条数刷盘) 默认同步刷盘
	FlushValue                 int           //刷盘阈值，达到阈值再进行刷盘
	SegmentSize                uint64        //文件的大小限制 默认64M 0 代表不限制
	Ft                         FileType      //文件类型(1 普通文件 2 mmap) 默认映射方式
	LogFileLimitForPurge       int           //日志文件数量限制
	LogEntryCountLimitForPurge int           //日志条目数量限制
	FilePrefix                 string  //日志文件的前缀 
	FileExtension              string //日志文件的后缀 默认wal
}
```
2. 如果需要对日志对象进行序列化和反序列操作，则需要注册Coder
   ```
   type Coder interface {
	    Type() int8 //标识编码器的类型，用于编码此类型标识的对象
	    Encode(interface{}) ([]byte, error)
	    Decode([]byte) (interface{}, error)
    }
    RegisterCoder(Coder) error //将编码器注册到lws
    注：type:<=0代表lws系统占用类型，其中0 代表的是字节对象的类型
   ```
  
3. 使用实例
   * 实例1
   ```
    l, err := Open("/root/go/src/chainmaker.org/lws/log", WithSegmentSize(30), WithFilePrex("test_"), WithFlushStrategy(FlushStrategyManual, 0), WithFileLimitForPurge(3))
	if err != nil {
        return err
    }
	data := []byte("hello world")
	err = l.Write(0, data)  //因为data为字节数组，故类型为0
	if err != nil {
        fmt.Println("write error:", err)
    }
	l.Flush()

    it := l.NewLogIterator()
	for it.HasNext() {
		data, err := it.Next().Get()
		if err != nil {
			fmt.Println("get error:", err)
		} else {
			fmt.Println("data:", string(data))
		}
	}
   
	l.Close()
   ```
   *实例2
   ```
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

    err := RegisterCoder(&StudentCoder{})
	require.Nil(t, err)
	l, err := Open("/root/go/src/chainmaker.org/lws/log", WithSegmentSize(30), WithWriteFileType(FileTypeMmap), WithFilePrex("test_"))
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
	it.SkipToLast()  //游标跳转至最新
	for i := 0; it.HasPre() && i < 5; i++ { //从后往前遍历
		obj, err := it.Previous().GetObj() //获取到解码后的对象
		if err != nil {
			t.Log("err:", err)
		} else {
			t.Log(obj)
		}
	}
	l.Close()
   ```



