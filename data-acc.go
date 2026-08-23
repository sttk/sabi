// Copyright (C) 2023-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"github.com/sttk/errs"
)

type /* error reasons */ (
	// FailToSetupGlobalDataSrcs represents an error reason indicating that one or more
	// global data sources failed to initialize during their setup phase. It wraps the
	// list of individual errors encountered by the data sources.
	FailToSetupGlobalDataSrcs struct {
		// Errors is a slice of ErrEntry containing errors encountered during setup.
		Errors []ErrEntry
	}

	// FailToSetupLocalDataSrcs represents an error reason indicating that one or more
	// local data sources registered to a specific DataHub failed to initialize when the
	// hub began transaction execution. It wraps the list of individual initialization errors.
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

	// FailToCreateDataConn represents an error reason indicating that a registered data source
	// encountered an error while attempting to establish or instantiate a new data connection.
	FailToCreateDataConn struct {
		// Name is the registered name of the data source.
		Name string
		// DataConnType is the type name of the data connection being created.
		DataConnType string
	}

	// CreatedDataConnIsNil represents an error reason indicating that the data source's connection
	// instantiation completed without returning an error, but the returned connection object was nil.
	CreatedDataConnIsNil struct {
		// Name is the registered name of the data source.
		Name string
		// DataConnType is the type name of the expected data connection.
		DataConnType string
	}

	// FailToCastDataConn represents an error reason indicating that a data connection was
	// successfully retrieved, but could not be type-cast to the specific implementation expected by
	// the caller.
	FailToCastDataConn struct {
		// Name is the registered name of the data connection.
		Name string
		// FromDataConnType is the actual type name of the retrieved connection.
		FromDataConnType string
		// ToDataConnType is the expected type name that the connection failed to cast to.
		ToDataConnType string
	}
)

var (
	globalDataSrcManager dataSrcManager = newDataSrcManager(false)
	globalDataSrcsFixed  bool           = false
)

// Uses registers a global data source with a unique identifier. This registration must occur
// before Setup is called, as global data sources are initialized during the Setup phase and
// shared across DataHub instances.
func Uses(name string, ds DataSrc) {
	if !globalDataSrcsFixed {
		globalDataSrcManager.add(name, ds)
	}
}

// Setup initializes all registered global data sources. It locks the global data sources to
// prevent further registrations. If any data source setup fails, it shuts down all successfully
// initialized data sources and returns an error wrapper.
func Setup() errs.Err {
	if !globalDataSrcsFixed {
		globalDataSrcsFixed = true

		errors := globalDataSrcManager.setup()
		if len(errors) > 0 {
			globalDataSrcManager.close()
			return errs.New(FailToSetupGlobalDataSrcs{Errors: errors})
		}
	}

	return errs.Ok()
}

// SetupWithOrder initializes all registered global data sources in the specific order defined by
// the provided names. Data sources not specified in the list are initialized after the ordered
// ones.
// If initialization fails, it shuts down all successfully initialized data sources and returns an
// error.
func SetupWithOrder(names ...string) errs.Err {
	if !globalDataSrcsFixed {
		globalDataSrcsFixed = true

		errors := globalDataSrcManager.setupWithOrder(names)
		if len(errors) > 0 {
			globalDataSrcManager.close()
			return errs.New(FailToSetupGlobalDataSrcs{Errors: errors})
		}
	}

	return errs.Ok()
}

// Shutdown cleans up and closes all global data sources that were successfully initialized,
// releasing resources like connection pools.
func Shutdown() {
	globalDataSrcManager.close()
}

// DataAcc is the foundational component for managing data sources and connections in a session
// or transaction context.
//
// It maintains local data sources, cached global data sources, and active data connections.
// Typically, user-defined data access structs embed DataAcc to gain access to data connections
// via GetDataConn while providing domain-specific data operations to the business logic layer.
type DataAcc struct {
	localDataSrcManager dataSrcManager
	dataSrcMap          map[string]dataSrcContainer
	dataConnManager     dataConnManager
	dataConnMap         map[string]dataConnContainer
	fixed               bool
}

// NewDataAcc creates and initializes a new DataAcc instance.
//
// It fixes the global data source registrations to prevent subsequent modifications,
// copies all ready global data sources into its local lookup table, and sets up empty
// local data source and connection managers.
func NewDataAcc() *DataAcc {
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

// NewDataAccWithCommitOrder creates and initializes a new DataAcc instance with a specified
// commit order for data connections.
//
// The provided names define the sequence in which data connections will be committed when a
// transaction finishes. If duplicate names are specified, earlier occurrences take precedence.
// Connections not specified in the list will be committed after the ordered ones.
func NewDataAccWithCommitOrder(names ...string) *DataAcc {
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

func (da *DataAcc) getDataAcc() *DataAcc {
	return da
}

func (da *DataAcc) uses(name string, ds DataSrc) {
	if da.fixed {
		return
	}

	da.localDataSrcManager.add(name, ds)
}

func (da *DataAcc) disuses(name string) {
	if da.fixed {
		return
	}

	if cont, ok := da.dataSrcMap[name]; ok {
		if cont.local {
			delete(da.dataSrcMap, name)
		}
	}

	da.localDataSrcManager.remove(name)
}

func (da *DataAcc) close() {
	if da.fixed {
		return
	}
	clear(da.dataConnMap)
	da.dataConnManager.close()
	clear(da.dataSrcMap)
	da.localDataSrcManager.close()
}

func (da *DataAcc) begin() errs.Err {
	da.fixed = true

	errors := da.localDataSrcManager.setup()
	if len(errors) > 0 {
		return errs.New(FailToSetupLocalDataSrcs{Errors: errors})
	}

	da.localDataSrcManager.copyDsReadyToMap(da.dataSrcMap)
	return errs.Ok()
}

func (da *DataAcc) commitOrRollback(err errs.Err) errs.Err {
	reports := da.dataConnManager.newFailureReports()
	if err.IsOk() {
		err = da.dataConnManager.commit(reports)
	}
	if err.IsNotOk() {
		da.dataConnManager.rollback(reports)
	}
	return err
}

func (da *DataAcc) end() {
	clear(da.dataConnMap)
	da.dataConnManager.close()

	da.fixed = false
}

// GetDataConn retrieves an existing DataConn by name from the cache, or creates a new one
// from the registered data source if it has not yet been instantiated.
//
// The generic type parameter C specifies the expected concrete or interface type of DataConn.
// If the connection is found or created but cannot be cast to C, or if the data source is not
// found or fails to create a connection, an error is returned.
func (da *DataAcc) GetDataConn[C DataConn](name string) (C, errs.Err) {
	dcCont, ok := da.dataConnMap[name]
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

	dsCont, ok := da.dataSrcMap[name]
	if !ok {
		return *new(C), errs.New(NoDataSrcToCreateDataConn{
			Name:         name,
			DataConnType: typeNameOfTypeParam[C](),
		})
	}

	dc0, err := dsCont.ds.CreateDataConn()
	if err.IsNotOk() {
		return *new(C), errs.New(FailToCreateDataConn{
			Name:         name,
			DataConnType: typeNameOfTypeParam[C](),
		}, err)
	}
	if dc0 == nil {
		return *new(C), errs.New(CreatedDataConnIsNil{
			Name:         name,
			DataConnType: typeNameOfTypeParam[C](),
		})
	}
	dc, ok := dc0.(C)
	if !ok {
		return *new(C), errs.New(FailToCastDataConn{
			Name:             name,
			FromDataConnType: typeNameOf(dc0),
			ToDataConnType:   typeNameOfTypeParam[C](),
		})
	}

	dcCont = dataConnContainer{name: name, conn: dc}
	da.dataConnMap[name] = dcCont
	da.dataConnManager.add(dcCont)

	return dc, errs.Ok()
}
