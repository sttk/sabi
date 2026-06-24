// Copyright (C) 2023-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"context"

	"github.com/sttk/errs"
)

type /* error reasons */ (
	// FailToSetupGlobalDataSrcs represents an error reason indicating that one or more
	// global data sources failed to initialize during their setup phase. It wraps the
	// list of individual errors encountered by the data sources.
	FailToSetupGlobalDataSrcs struct {
		Errors []DataSrcErr
	}

	// FailToSetupLocalDataSrcs represents an error reason indicating that one or more
	// local data sources registered to a specific DataHub failed to initialize when the
	// hub began transaction execution. It wraps the list of individual initialization errors.
	FailToSetupLocalDataSrcs struct {
		Errors []DataSrcErr
	}

	// NoDataSrcToCreateDataConn represents an error reason indicating that there is no
	// registered data source matching the requested name, making it impossible to create
	// the requested data connection.
	NoDataSrcToCreateDataConn struct {
		Name         string
		DataConnType string
	}

	// FailToCreateDataConn represents an error reason indicating that a registered data source
	// encountered an error while attempting to establish or instantiate a new data connection.
	FailToCreateDataConn struct {
		Name         string
		DataConnType string
	}

	// CreatedDataConnIsNil represents an error reason indicating that the data source's connection
	// instantiation completed without returning an error, but the returned connection object was nil.
	CreatedDataConnIsNil struct {
		Name         string
		DataConnType string
	}

	// FailToCastDataConn represents an error reason indicating that a data connection was
	// successfully retrieved, but could not be type-cast to the specific implementation expected by
	// the caller.
	FailToCastDataConn struct {
		Name           string
		ToDataConnType string
	}

	// FailToCastDataHub represents an error reason indicating that the provided DataHub instance
	// could not be type-cast to the generic data access interface type required by the run or
	// transaction logic.
	FailToCastDataHub struct {
		FromType string
		ToType   string
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
// If initialization fails, it shuts down all successfully initialized data sources and returns an error.
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

// DataHub defines the interface for a coordinator that manages the lifecycle of local data sources,
// manages active data connections, and facilitates the execution of transactional logic. It extends
// the DataAcc interface to allow querying and retrieving active data connections.
type DataHub interface {
	DataAcc

	// Uses registers a local data source with a unique identifier specifically for this DataHub
	// instance.
	// This local data source is only visible within this hub's execution scope.
	Uses(name string, ds DataSrc)
	// Disuses removes a registered local data source from this DataHub instance, or marks a global
	// data source as ignored in this hub's context.
	Disuses(name string)
	// Close releases all local resources, connections, and data sources managed by this DataHub.
	Close()

	begin() errs.Err
	commitOrRollback(errs.Err) errs.Err
	end()
}

type dataHubImpl struct {
	DataHub

	localDataSrcManager dataSrcManager
	dataSrcMap          map[string]dataSrcContainer
	dataConnManager     dataConnManager
	dataConnMap         map[string]dataConnContainer
	fixed               bool

	origCtx context.Context
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewDataHub creates and initializes a new DataHub instance populated with the currently
// ready global data sources. The returned hub can be configured with additional local data sources
// prior to executing logic.
func NewDataHub() DataHub {
	globalDataSrcsFixed = true

	dsMap := make(map[string]dataSrcContainer, len(globalDataSrcManager.listReady))
	globalDataSrcManager.copyDsReadyToMap(dsMap)

	return &dataHubImpl{
		localDataSrcManager: newDataSrcManager(true),
		dataSrcMap:          dsMap,
		dataConnManager:     newDataConnManager(),
		dataConnMap:         make(map[string]dataConnContainer),
		fixed:               false,
	}
}

// NewDataHubWithCommitOrder creates and initializes a new DataHub instance, specifying a sequence
// in which its data connections should be committed. This helps ensure multi-resource consistency
// when certain connections depend on the successful commit of others.
func NewDataHubWithCommitOrder(names ...string) DataHub {
	globalDataSrcsFixed = true

	dsMap := make(map[string]dataSrcContainer, len(globalDataSrcManager.listReady))
	globalDataSrcManager.copyDsReadyToMap(dsMap)

	return &dataHubImpl{
		localDataSrcManager: newDataSrcManager(true),
		dataSrcMap:          dsMap,
		dataConnManager:     newDataConnManagerWithCommitOrder(names),
		dataConnMap:         make(map[string]dataConnContainer),
		fixed:               false,
	}
}

func (hub *dataHubImpl) Uses(name string, ds DataSrc) {
	if hub.fixed {
		return
	}

	hub.localDataSrcManager.add(name, ds)
}

func (hub *dataHubImpl) Disuses(name string) {
	if hub.fixed {
		return
	}

	if cont, ok := hub.dataSrcMap[name]; ok {
		if cont.local {
			delete(hub.dataSrcMap, name)
		}
	}

	hub.localDataSrcManager.remove(name)
}

func (hub *dataHubImpl) Close() {
	if hub.fixed {
		return
	}
	hub.origCtx = nil
	clear(hub.dataConnMap)
	hub.dataConnManager.close()
	clear(hub.dataSrcMap)
	hub.localDataSrcManager.close()
}

func (hub *dataHubImpl) begin() errs.Err {
	hub.fixed = true

	if hub.origCtx != nil {
		hub.ctx, hub.cancel = context.WithCancel(hub.origCtx)
	}

	errors := hub.localDataSrcManager.setup()
	if len(errors) > 0 {
		return errs.New(FailToSetupLocalDataSrcs{Errors: errors})
	}

	hub.localDataSrcManager.copyDsReadyToMap(hub.dataSrcMap)
	return errs.Ok()
}

func (hub *dataHubImpl) commitOrRollback(err errs.Err) errs.Err {
	return hub.dataConnManager.commitOrRollback(err)
}

func (hub *dataHubImpl) end() {
	if hub.cancel != nil {
		cancel := hub.cancel
		hub.cancel = nil
		cancel()
	}
	if hub.ctx != nil {
		hub.ctx = nil
	}

	clear(hub.dataConnMap)
	hub.dataConnManager.close()

	hub.fixed = false
}

func (hub *dataHubImpl) Context() context.Context {
	return hub.ctx
}

func (hub *dataHubImpl) SetContext(ctx context.Context) {
	hub.origCtx = ctx
}

func (hub *dataHubImpl) getDataConn(name string, dataConnType string) (DataConn, errs.Err) {
	dcCont, ok := hub.dataConnMap[name]
	if ok {
		return dcCont.conn, errs.Ok()
	}

	dsCont, ok := hub.dataSrcMap[name]
	if !ok {
		return nil, errs.New(NoDataSrcToCreateDataConn{Name: name, DataConnType: dataConnType})
	}

	dc, err := dsCont.ds.CreateDataConn()
	if err.IsNotOk() {
		return nil, errs.New(FailToCreateDataConn{Name: name, DataConnType: dataConnType}, err)
	}
	if dc == nil {
		return nil, errs.New(CreatedDataConnIsNil{Name: name, DataConnType: dataConnType})
	}

	dcCont = dataConnContainer{name: name, conn: dc}
	hub.dataConnMap[name] = dcCont
	hub.dataConnManager.add(dcCont)

	return dc, errs.Ok()
}

// GetDataConn retrieves an active connection of the specified type from the provided data
// container.
// It searches the container's DataHub, instantiating the connection from the registered data source
// if it does not yet exist, and casts it to the expected interface type.
func GetDataConn[C DataConn](data any, name string) (C, errs.Err) {
	hub := data.(DataAcc)

	toType := typeNameOfTypeParam[C]()

	dc, err := hub.getDataConn(name, toType)
	if err.IsNotOk() {
		return *new(C), err
	}

	c, ok := dc.(C)
	if !ok {
		return *new(C), errs.New(FailToCastDataConn{Name: name, ToDataConnType: toType})
	}

	return c, errs.Ok()
}

// Run executes a non-transactional business logic function using the provided DataHub.
// It manages the hub's lifecycle by starting its local data sources before running the logic,
// and ensures proper resource cleanup upon completion. It returns an error if setup or the logic
// fails.
func Run[D any](hub DataHub, logic func(D) errs.Err) errs.Err {
	data, ok := hub.(D)
	if !ok {
		fromType := typeNameOf(&hub)[1:]
		toType := typeNameOfTypeParam[D]()
		return errs.New(FailToCastDataHub{FromType: fromType, ToType: toType})
	}

	err := hub.begin()
	if err.IsNotOk() {
		return err
	}
	defer hub.end()

	return logic(data)
}

// Txn executes a transactional business logic function using the provided DataHub.
// It manages the hub's lifecycle, starting data sources, running the logic, and automatically
// committing the changes if the logic succeeds, or rolling back if an error occurs.
func Txn[D any](hub DataHub, logic func(D) errs.Err) errs.Err {
	data, ok := hub.(D)
	if !ok {
		fromType := typeNameOf(&hub)[1:]
		toType := typeNameOfTypeParam[D]()
		return errs.New(FailToCastDataHub{FromType: fromType, ToType: toType})
	}

	err := hub.begin()
	if err.IsNotOk() {
		return err
	}
	defer hub.end()

	err = logic(data)
	return hub.commitOrRollback(err)
}
