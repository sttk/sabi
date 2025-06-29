// Copyright (C) 2023-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import "github.com/sttk/errs"

type DataAcc interface {
	getDataConn(name, dataConnType string) (DataConn, errs.Err)
}
