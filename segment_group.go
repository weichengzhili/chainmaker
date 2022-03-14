/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
package lws

import "sync"

type SegmentGroup []*Segment

func (sg *SegmentGroup) At(i int) *Segment {
	return (*sg)[i]
}

func (sg *SegmentGroup) Len() int {
	return len(*sg)
}

func (sg *SegmentGroup) Cap() int {
	return cap(*sg)
}

func (sg *SegmentGroup) Append(s *Segment) {
	*sg = append(*sg, s)
}

func (sg *SegmentGroup) First() *Segment {
	return (*sg)[0]
}

func (sg *SegmentGroup) Assign(i int, s *Segment) {
	(*sg)[i] = s
}

func (sg *SegmentGroup) Last() *Segment {
	return (*sg)[sg.Len()-1]
}

func (sg *SegmentGroup) Reserved(n int) {
	if n > sg.Cap() {
		new := make([]*Segment, sg.Len(), n)
		copy(new, *sg)
		*sg = new
	}
}

func (sg *SegmentGroup) Resize(n int) {
	sg.Reserved(n)
	*sg = (*sg)[:n]
}

func (sg *SegmentGroup) Traverse(fn func(i int, s *Segment) bool) {
	for i, s := range *sg {
		if fn(i, s) {
			return
		}
	}
}

func (sg *SegmentGroup) Split(i int) (SegmentGroup, SegmentGroup) {
	ret := *sg
	if i > sg.Len() {
		return ret, nil
	}
	return ret[:i], ret[i:]
}

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

type rwlockSegmentGroup struct {
	SegmentGroup
	sync.RWMutex
}
