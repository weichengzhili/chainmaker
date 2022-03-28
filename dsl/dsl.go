/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package dsl

import (
	"errors"
	"path/filepath"
	"strings"
)

var (
	protoDelimiter = "://"
	ErrFormat      = errors.New("invalid protocol format")
	ErrNotSupport  = errors.New("unsupported protocol")
	ErrInvaildPath = errors.New("invaild path")
)

//DSL data store location
type DSL struct {
	Schema string
	Path   string
}

//Parse parse string p to a DSL
func Parse(p string) (*DSL, error) {
	if len(p) == 0 {
		return nil, ErrInvaildPath
	}
	ps := strings.Split(p, protoDelimiter)
	l := len(ps)
	if l > 2 {
		return nil, ErrFormat
	}
	//if has no prefix xxx://，maybe it's a local file path
	if l == 1 {
		var (
			p   = ps[0]
			err error
		)
		if !filepath.IsAbs(ps[0]) {
			p, err = filepath.Abs(ps[0])
			if err != nil {
				return nil, err
			}
		}
		return &DSL{
			Schema: "file",
			Path:   p,
		}, nil

	}
	return &DSL{
		Schema: strings.ToLower(ps[0]),
		Path:   ps[1],
	}, nil
}
