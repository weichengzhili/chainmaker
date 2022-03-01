/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/

package lws

type writeNoticeType int8

const (
	newFile writeNoticeType = 1 << iota
	newLog
)
