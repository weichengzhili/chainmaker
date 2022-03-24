/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package dsl

type (
	ProtocolType int
	Protocol     struct {
		Type ProtocolType
		Name string
	}
)

const (
	PT_UNKNOWN ProtocolType = iota
	PT_FILE
)

var (
	supportProtocols protocols = []Protocol{
		{
			Type: PT_FILE,
			Name: "file",
		},
	}
)

type protocols []Protocol

func (ps *protocols) GetProtocolType(name string) ProtocolType {
	for _, p := range *ps {
		if p.Name == name {
			return p.Type
		}
	}
	return PT_UNKNOWN
}

func (ps *protocols) HasName(name string) bool {
	for _, p := range *ps {
		if p.Name == name {
			return true
		}
	}
	return false
}

func (ps *protocols) HasType(t ProtocolType) bool {
	for _, p := range *ps {
		if p.Type == t {
			return true
		}
	}
	return false
}

func ProtocolTypeBySchema(schema string) ProtocolType {
	return supportProtocols.GetProtocolType(schema)
}

func IsSupportedForSchema(schema string) bool {
	return supportProtocols.HasName(schema)
}

func IsSupportedForType(t ProtocolType) bool {
	return supportProtocols.HasType(t)
}
