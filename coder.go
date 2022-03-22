/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/

package lws

import (
	"encoding/json"
	"errors"
	"sync"
)

var (
	coderMapping = make(map[int8]Coder)
	locker       sync.Mutex

	ErrCoderExist    = errors.New("this type coder has exist")
	ErrCoderNotExist = errors.New("this type coder not exist")
	ErrCodeSysType   = errors.New("the coder type is system reservation type")

	RawCoderType    int8 = 0
	DefaultJsonType int8 = -128
)

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

type Coder interface {
	Type() int8
	Encode(interface{}) ([]byte, error)
	Decode([]byte) (interface{}, error)
}

func checkCoderType(t int8) error {
	if t <= RawCoderType {
		return ErrCodeSysType
	}
	return nil
}

func RegisterCoder(c Coder) error {
	if err := checkCoderType(c.Type()); err != nil {
		return err
	}
	locker.Lock()
	defer locker.Unlock()
	if _, exist := coderMapping[c.Type()]; exist {
		return ErrCoderExist
	}
	coderMapping[c.Type()] = c
	return nil
}

func GetCoder(t int8) (Coder, error) {
	locker.Lock()
	defer locker.Unlock()
	if c, exist := coderMapping[t]; exist {
		return c, nil
	}
	return nil, ErrCoderNotExist
}

type JsonCoder struct {
}

func (j *JsonCoder) Type() int8 {
	return DefaultJsonType
}

func (j *JsonCoder) Encode(obj interface{}) ([]byte, error) {
	return json.Marshal(obj)
}
func (j *JsonCoder) Decode(d []byte) (interface{}, error) {
	return d, nil
}
