// Copyright (C) 2023-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import "github.com/sttk/errs"

// The interface that aggregates data access operations to external data services
// into logical units, with methods providing default implementations.
//
// The organization of these units is flexible; it can be per data service, per
// functional area, or any other meaningful grouping. Implementations of this interface
// use the GetDataConn function to obtain a DataConn object by the name specified
// during data source registration (via the global Uses function or DataHub#Uses method).
// This DataConn then facilitates data access operations to the associated data service.
//
// Methods declared in DataAcc interfaces can be overridden by *Data* interfaces which will be
// passed to logic functions as their arguments.
// This design allows for the separation of data input/output logic into specific DataAcc
// implementations, while DataHub aggregates all these methods.
// Logic functions, however, only see the methods declared in the *Data* interface,
// enabling a clear separation and aggregation of data input/output methods.
type DataAcc interface {
	getDataConn(name, dataConnType string) (DataConn, errs.Err)
}
