// Copyright (C) 2023-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"github.com/sttk/errs"
)

// DataAcc is an interface that provides access to data connections
// within a transactional or logic execution scope. It acts as an abstraction layer for
// retrieving active database or resource connections by name and type, allowing data access
// objects or repositories to interact with resources without knowing the details of the
// underlying transaction coordinator.
type DataAcc interface {
	getDataConn(name, dataConnType string) (DataConn, errs.Err)
}
