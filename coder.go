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

	DefaultJsonType = int8(-128)
)

type Coder interface {
	Type() int8
	Encode(interface{}) ([]byte, error)
	Decode([]byte) (interface{}, error)
}

func RegisterCoder(c Coder) error {
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
