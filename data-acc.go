// Copyright (C) 2023-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"github.com/sttk/errs"
)

type /* error reasons */ (
	// FailToSetupLocalDataSrcs represents an error reason indicating that one or more
	// local data sources registered to a DataAcc (or LogicRunner) failed to initialize during
	// their setup phase. It wraps the list of individual initialization errors.
	FailToSetupLocalDataSrcs struct {
		// Errors is a slice of ErrEntry containing errors encountered during setup.
		Errors []ErrEntry
	}

	// NoDataSrcToCreateDataConn represents an error reason indicating that there is no
	// registered data source matching the requested name, making it impossible to create
	// the requested data connection.
	NoDataSrcToCreateDataConn struct {
		// Name is the registered name of the requested data source.
		Name string
		// DataConnType is the type name of the requested data connection.
		DataConnType string
	}

	// CreatedDataConnIsNil represents an error reason indicating that the data source's
	// connection instantiation completed without returning an error, but the returned
	// connection object was nil.
	CreatedDataConnIsNil struct {
		// Name is the registered name of the data source.
		Name string
		// DataConnType is the type name of the expected data connection.
		DataConnType string
	}

	// FailToCreateDataConn represents an error reason indicating that a registered data source
	// encountered an error while attempting to establish or instantiate a new data connection.
	FailToCreateDataConn struct {
		// Name is the registered name of the data source.
		Name string
		// DataConnType is the type name of the data connection being created.
		DataConnType string
	}

	// CreatedDataConn represents an error reason indicating that a data connection has already
	// been created for the specified name.
	CreatedDataConn struct {
		// Name is the registered name of the data connection.
		Name string
		// DataConnType is the type name of the created data connection.
		DataConnType string
	}

	// FailToCastDataConn represents an error reason indicating that a data connection was
	// successfully retrieved, but could not be type-cast to the specific implementation
	// expected by the caller.
	FailToCastDataConn struct {
		// Name is the registered name of the data connection.
		Name string
		// FromDataConnType is the actual type name of the retrieved connection.
		FromDataConnType string
		// ToDataConnType is the expected type name that the connection failed to cast to.
		ToDataConnType string
	}

	// FailToCastDataHub represents an error reason indicating that the user-defined DataHub
	// object could not be type-cast to the data interface type required by the logic function.
	FailToCastDataHub struct {
		// Name is an optional identifier or label for the data hub or logic.
		Name string
		// FromType is the actual type name of the DataHub object being cast.
		FromType string
		// ToType is the expected data interface type name that the cast failed to match.
		ToType string
	}
)

// DataAcc provides access to data connections and coordinates data sources within a logic
// execution scope. It manages the creation, caching, and retrieval of DataConn instances
// by name and type, and supports executing nested logic blocks.
type DataAcc struct {
	localDataSrcManager dataSrcManager
	dataSrcMap          map[string]dataSrcContainer
	dataConnManager     dataConnManager
	dataConnMap         map[string]dataConnContainer
	fixed               bool
	dataHub             any
}

func newDataAcc() *DataAcc {
	globalDataSrcsFixed = true

	dsMap := make(map[string]dataSrcContainer, len(globalDataSrcManager.listReady))
	globalDataSrcManager.copyDsReadyToMap(dsMap)

	return &DataAcc{
		localDataSrcManager: newDataSrcManager(true),
		dataSrcMap:          dsMap,
		dataConnManager:     newDataConnManager(),
		dataConnMap:         make(map[string]dataConnContainer),
		fixed:               false,
	}
}

func newDataAccWithCommitOrder(names []string) *DataAcc {
	globalDataSrcsFixed = true

	dsMap := make(map[string]dataSrcContainer, len(globalDataSrcManager.listReady))
	globalDataSrcManager.copyDsReadyToMap(dsMap)

	return &DataAcc{
		localDataSrcManager: newDataSrcManager(true),
		dataSrcMap:          dsMap,
		dataConnManager:     newDataConnManagerWithCommitOrder(names),
		dataConnMap:         make(map[string]dataConnContainer),
		fixed:               false,
	}
}

func (acc *DataAcc) uses(name string, ds DataSrc) {
	if acc.fixed {
		return
	}

	acc.localDataSrcManager.add(name, ds)
}

func (acc *DataAcc) disuses(name string) {
	if acc.fixed {
		return
	}

	if cont, ok := acc.dataSrcMap[name]; ok {
		if cont.local {
			delete(acc.dataSrcMap, name)
		}
	}

	acc.localDataSrcManager.remove(name)
}

func (acc *DataAcc) close() {
	clear(acc.dataConnMap)
	acc.dataConnManager.close()
	clear(acc.dataSrcMap)
	acc.localDataSrcManager.close()
}

func (acc *DataAcc) begin() errs.Err {
	acc.fixed = true

	errors := acc.localDataSrcManager.setup()
	if len(errors) > 0 {
		acc.fixed = false
		return errs.New(FailToSetupLocalDataSrcs{Errors: errors})
	}

	acc.localDataSrcManager.copyDsReadyToMap(acc.dataSrcMap)
	return errs.Ok()
}

func (acc *DataAcc) commitOrRollback(err errs.Err) errs.Err {
	reports := acc.dataConnManager.newFailureReports()
	if err.IsOk() {
		err = acc.dataConnManager.commit(reports)
	}
	if err.IsNotOk() {
		acc.dataConnManager.rollback(reports)
	}
	return err
}

func (acc *DataAcc) end() {
	clear(acc.dataConnMap)
	acc.dataConnManager.close()

	acc.fixed = false
}

// GetDataConn retrieves an active connection of the specified type C from the DataAcc.
// If the connection already exists in this scope, it is returned. Otherwise, it instantiates
// a new connection from the registered data source and caches it.
// It returns the connection and an error if no matching data source is found, connection
// creation fails, or the connection cannot be cast to type C.
func (acc *DataAcc) GetDataConn[C DataConn](name string) (C, errs.Err) {
	dcCont, ok := acc.dataConnMap[name]
	if ok {
		dc, ok := dcCont.conn.(C)
		if !ok {
			return *new(C), errs.New(FailToCastDataConn{
				Name:             name,
				FromDataConnType: typeNameOf(dcCont.conn),
				ToDataConnType:   typeNameOfTypeParam[C](),
			})
		}
		return dc, errs.Ok()
	}

	dsCont, ok := acc.dataSrcMap[name]
	if !ok {
		return *new(C), errs.New(NoDataSrcToCreateDataConn{
			Name:         name,
			DataConnType: typeNameOfTypeParam[C](),
		})
	}

	dcNew, err := dsCont.ds.CreateDataConn()
	if err.IsNotOk() {
		return *new(C), errs.New(FailToCreateDataConn{
			Name:         name,
			DataConnType: typeNameOfTypeParam[C](),
		}, err)
	}
	if dcNew == nil {
		return *new(C), errs.New(CreatedDataConnIsNil{
			Name:         name,
			DataConnType: typeNameOfTypeParam[C](),
		})
	}
	dc, ok := dcNew.(C)
	if !ok {
		dcNew.Close()
		return *new(C), errs.New(FailToCastDataConn{
			Name:             name,
			FromDataConnType: typeNameOf(dcNew),
			ToDataConnType:   typeNameOfTypeParam[C](),
		})
	}

	dcCont = dataConnContainer{name: name, conn: dc}
	acc.dataConnMap[name] = dcCont
	acc.dataConnManager.add(dcCont)

	return dc, errs.Ok()
}

// Run executes a nested business logic function using the DataHub object associated with
// this DataAcc. It type-casts the DataHub object to type D and passes it to the provided
// logic function.
// It returns an error if the DataHub cannot be cast to type D or if the logic function fails.
func (acc *DataAcc) Run[D any](logic func(D) errs.Err) errs.Err {
	data, ok := acc.dataHub.(D)
	if !ok {
		fromType := typeNameOf(acc.dataHub)
		toType := typeNameOfTypeParam[D]()
		return errs.New(FailToCastDataHub{FromType: fromType, ToType: toType})
	}

	return logic(data)
}

// Start begins a method chain for executing nested non-transactional business logic functions
// using LogicRun.
func (acc *DataAcc) Start() LogicRun {
	return newLogicRun(acc, true)
}
