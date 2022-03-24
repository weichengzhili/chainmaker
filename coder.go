/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package lws

import (
	"errors"
	"sync"
)

var (
	ErrCoderExist    = errors.New("this type coder has exist")
	ErrCoderNotExist = errors.New("this type coder not exist")
	ErrCodeSysType   = errors.New("the coder type is system reservation type")

	RawCoderType int8 = 0
	// DefaultJsonType int8 = -128
)

type Coder interface {
	Type() int8
	Encode(interface{}) ([]byte, error)
	Decode([]byte) (interface{}, error)
}
type coderMap struct {
	sync.Mutex
	m map[int8]Coder
}

func newCoderMap() *coderMap {
	return &coderMap{
		m: make(map[int8]Coder),
	}
}

func (cm *coderMap) RegisterCoder(c Coder) error {
	if err := checkCoderType(c.Type()); err != nil {
		return err
	}
	cm.Lock()
	defer cm.Unlock()
	if _, exist := cm.m[c.Type()]; exist {
		return ErrCoderExist
	}
	cm.m[c.Type()] = c
	return nil
}

func (cm *coderMap) UnregisterCoder(t int8) error {
	if err := checkCoderType(t); err != nil {
		return err
	}
	cm.Lock()
	defer cm.Unlock()
	if _, exist := cm.m[t]; !exist {
		return nil
	}
	delete(cm.m, t)
	return nil
}

func (cm *coderMap) GetCoder(t int8) (Coder, error) {
	cm.Lock()
	defer cm.Unlock()
	if c, exist := cm.m[t]; exist {
		return c, nil
	}
	return nil, ErrCoderNotExist
}

func checkCoderType(t int8) error {
	if t <= RawCoderType {
		return ErrCodeSysType
	}
	return nil
}
