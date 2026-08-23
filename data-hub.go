// Copyright (C) 2023-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"github.com/sttk/errs"
)

type /* error reasons */ (
	// FailToCastDataAcc represents an error reason indicating that the provided DataAcc instance
	// could not be type-cast to the generic data access interface type required by the run or
	// transaction logic.
	FailToCastDataAcc struct {
		// FromType is the type name of the DataAcc instance being cast.
		FromType string
		// ToType is the expected data access interface type name that the cast failed to match.
		ToType string
	}
)

// DataHub is the central coordinator for executing business logic with data access capabilities.
//
// It bridges user-defined data access implementations (which implement IDataAcc and domain-specific
// interfaces) with the underlying DataAcc instance. DataHub provides methods to register local data
// sources, release resources, and execute logic functions either without transaction management (Run)
// or within an atomic transaction boundary (Txn).
type DataHub struct {
	da  *DataAcc
	ida IDataAcc
}

// IDataAcc is an interface that exposes the underlying DataAcc instance.
//
// Structs representing customized data access hubs embed DataAcc or DataHub and implement
// this interface to allow DataHub to manage data sources and connections during execution.
type IDataAcc interface {
	getDataAcc() *DataAcc
}

// NewDataHub creates a new DataHub instance associated with the specified IDataAcc implementation.
//
// The provided ida instance is retained to allow casting to domain-specific data access interfaces
// when executing logic with Run or Txn.
func NewDataHub(ida IDataAcc) DataHub {
	return DataHub{
		da:  ida.getDataAcc(),
		ida: ida,
	}
}

// Uses registers a local data source with a unique identifier to the DataHub's underlying DataAcc.
//
// Local data sources are specific to this DataHub instance (e.g., for a session or request) and
// are initialized when Run or Txn begins execution.
func (hub DataHub) Uses(name string, ds DataSrc) {
	hub.da.uses(name, ds)
}

// Disuses unregisters and closes a local data source by name from the DataHub's underlying DataAcc.
//
// If a data source with the specified name exists and is local, it is closed and removed.
func (hub DataHub) Disuses(name string) {
	hub.da.disuses(name)
}

// Close releases all resources associated with the DataHub, closing any active data connections
// and registered local data sources.
func (hub DataHub) Close() {
	hub.da.close()
}

// Run executes the provided business logic function without transaction management.
//
// It type-casts the embedded data access instance to the generic type D (the data access interface
// expected by the logic function), initializes local data sources, and invokes the logic function.
// If the type-cast fails or initialization encounters an error, an error is returned.
func (hub DataHub) Run[D any](logic func(D) errs.Err) errs.Err {
	data, ok := hub.ida.(D)
	if !ok {
		fromType := typeNameOf(hub.da)
		toType := typeNameOfTypeParam[D]()
		return errs.New(FailToCastDataAcc{FromType: fromType, ToType: toType})
	}

	err := hub.da.begin()
	if err.IsNotOk() {
		return err
	}
	defer hub.da.end()

	return logic(data)
}

// Txn executes the provided business logic function within a managed transaction.
//
// It type-casts the embedded data access instance to the generic type D, initializes local data sources,
// and invokes the logic function. If the logic succeeds (returns errs.Ok()), all participating data
// connections are committed (with pre-commit, commit, and post-commit phases). If any error occurs
// during logic execution or commit phases, all connections are rolled back and OnTxnFailure is invoked.
func (hub DataHub) Txn[D any](logic func(D) errs.Err) errs.Err {
	data, ok := hub.ida.(D)
	if !ok {
		fromType := typeNameOf(hub.da)
		toType := typeNameOfTypeParam[D]()
		return errs.New(FailToCastDataAcc{FromType: fromType, ToType: toType})
	}

	err := hub.da.begin()
	if err.IsNotOk() {
		return err
	}
	defer hub.da.end()

	err = logic(data)
	return hub.da.commitOrRollback(err)
}
