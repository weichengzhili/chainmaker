/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package lws

import "sync"

type SegmentGroup []*Segment

//At 获取为i的数据，请确保i在[0,len-1)范围内
func (sg *SegmentGroup) At(i int) *Segment {
	return (*sg)[i]
}

func (sg *SegmentGroup) Len() int {
	return len(*sg)
}

func (sg *SegmentGroup) Cap() int {
	return cap(*sg)
}

//Append 追加条目
func (sg *SegmentGroup) Append(s *Segment) {
	*sg = append(*sg, s)
}

func (sg *SegmentGroup) First() *Segment {
	return (*sg)[0]
}

//Assign 将索引i指定的值由s重新赋值
func (sg *SegmentGroup) Assign(i int, s *Segment) {
	(*sg)[i] = s
}

func (sg *SegmentGroup) Last() *Segment {
	return (*sg)[sg.Len()-1]
}

//Reserved 预留，会影响SegmentGroup的cap大小
func (sg *SegmentGroup) Reserved(n int) {
	if n > sg.Cap() {
		new := make([]*Segment, sg.Len(), n)
		copy(new, *sg)
		*sg = new
	}
}

//Resize 预分配，会影响SegmentGroup的len大小
func (sg *SegmentGroup) Resize(n int) {
	sg.Reserved(n)
	*sg = (*sg)[:n]
}

//ForEach 遍历SegmentGroup所有元素，并调用fn，fn返回true标识遍历终止
func (sg *SegmentGroup) ForEach(fn func(i int, s *Segment) bool) {
	for i, s := range *sg {
		if fn(i, s) {
			return
		}
	}
}

//Split 分割，将SegmentGroup分割成[0,i),[i:len)两部分，如果i大于len，则返回[0,len), nil
func (sg *SegmentGroup) Split(i int) (SegmentGroup, SegmentGroup) {
	ret := *sg
	if i > sg.Len() {
		return ret, nil
	}
	return ret[:i], ret[i:]
}

//FindAt 通过二分查找，找到idx所在的sgement信息
func (sg *SegmentGroup) FindAt(idx uint64) *Segment {
	if idx < sg.First().Index {
		return nil
	}
	b, e := 0, sg.Len()
	for b < e {
		m := (e + b) / 2
		if idx >= sg.At(m).Index {
			b = m + 1
		} else {
			e = m
		}
	}
	return sg.At(b - 1)
}

//带锁的SegmentGroup
type rwlockSegmentGroup struct {
	SegmentGroup
	sync.RWMutex
}
