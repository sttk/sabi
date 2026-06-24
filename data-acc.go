// Copyright (C) 2023-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"context"

	"github.com/sttk/errs"
)

// DataAcc is an interface that provides access to data connections and execution context
// within a transactional or logic execution scope. It acts as an abstraction layer for
// retrieving active database or resource connections by name and type, allowing data access
// objects or repositories to interact with resources without knowing the details of the
// underlying transaction coordinator.
type DataAcc interface {
	getDataConn(name, dataConnType string) (DataConn, errs.Err)

	setContext(ctx context.Context)

	// Context returns the context.Context associated with the current execution scope,
	// which can be used to control timeouts, cancellation, or pass request-scoped values
	// down to data connections or queries.
	Context() context.Context
}
