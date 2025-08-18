// Copyright (C) 2023-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"maps"

	"github.com/sttk/errs"
)

// A struct type representing the reasons for errors that can occur within DataHub operations.
type /* error reasons */ (
	// Indicates a failure during the setup process of one or more global data sources.
	// Contains a map of data source names to their corresponding errors.
	FailToSetupGlobalDataSrcs struct {
		// The map contains errors that occurred in each DataSrc object.
		Errors map[string]errs.Err
	}

	// Indicates a failure during the setup process of one or more session-local data sources.
	// Contains a map of data source names to their corresponding errors.
	FailToSetupLocalDataSrcs struct {
		// The map contains errors that occurred in each DataSrc object.
		Errors map[string]errs.Err
	}

	// Indicates a failure during the commit process of one or more DataConn instances
	// involved in a transaction. Contains a map of data connection names to their errors.
	FailToCommitDataConn struct {
		// The map contains errors that occurred in each DataConn object.
		Errors map[string]errs.Err
	}

	// Indicates a failure during the pre commit process of one or more DataConn instances
	// involved in a transaction. Contains a map of data connection names to their errors.
	FailToPreCommitDataConn struct {
		// The map contains errors that occurred in each `DataConn` object.
		Errors map[string]errs.Err
	}

	// Indicates that no DataSrc was found to create a DataConn for the specified name
	// and type.
	NoDataSrcToCreateDataConn struct {
		// The name of the data source that could not be found.
		Name string

		// The type name of the `DataConn` that was requested.
		DataConnType string
	}

	// Indicates a failure to create a DataConn object.
	FailToCreateDataConn struct {
		// The name of the data source that failed to be created.
		Name string

		// The type name of the DataConn that failed to be created.
		DataConnType string
	}

	// Indicates a failure that the created DataConn instance is nil.
	CreatedDataConnIsNil struct {
		// The name of the data connection that is nil after creation.
		Name string

		// The type name of the data connection expected.
		DataConnType string
	}

	// Indicates a failure to cast a retrieved DataConn to the expected type.
	FailToCastDataConn struct {
		// The name of the data connection that failed to cast.
		Name string

		// The type name to which the DataConn attempted to cast.
		CastToType string
	}

	// Indicates a failure to cast a given DataHub to the expected type.
	FailToCastDataHub struct {
		// The name of the data hub that failed to cast.
		CastFromType string

		// The type name to which the DataHub attempted to cast.
		CastToType string
	}
)

var (
	globalDataSrcList   dataSrcList = dataSrcList{local: false}
	globalDataSrcsFixed bool        = false
)

// Registers a global data source that can be used throughout the application.
//
// This function associates a given DataSrc implementation with a unique name.
// This name will later be used to retrieve session-specific DataConn instances
// from this data source.
//
// Global data sources are set up once via the Setup function and are available
// to all DataHub instances.
func Uses(name string, ds DataSrc) {
	if !globalDataSrcsFixed {
		globalDataSrcList.addDataSrc(name, ds)
	}
}

// Executes the setup process for all globally registered data sources.
//
// This setup typically involves tasks such as creating connection pools,
// opening global connections, or performing initial configurations necessary
// for creating session-specific connections. The setup can run synchronously
// or asynchronously using an AsyncGroup if operations are time-consuming.
//
// If any data source fails to set up, this function returns an errs.Err with
// FailToSetupGlobalDataSrcs, containing a map of the names
// of the failed data sources and their corresponding errs.Err objects. In such a case,
// all global data sources that were successfully set up are also closed.
//
// If all data source setups are successful, the errs.Ok is returned.
func Setup() errs.Err {
	if !globalDataSrcsFixed {
		globalDataSrcsFixed = true

		errMap := globalDataSrcList.setupDataSrcs()
		if len(errMap) > 0 {
			globalDataSrcList.closeDataSrcs()
			return errs.New(FailToSetupGlobalDataSrcs{Errors: errMap})
		}
	}
	return errs.Ok()
}

// Closes and removes all global data sources.
func Shutdown() {
	globalDataSrcList.closeDataSrcs()
}

// The struct that acts as a central hub for data input/output operations, integrating
// multiple *Data* interface (which are passed to business logic functions as their arguments) with
// `DataAcc` inherited struct (which implement data I/O methods for external services).
//
// It facilitates data access by providing DataConn objects, created from
// both global data sources (registered via the global Uses function) and
// session-local data sources (registered via DataHub#Uses method).
//
// The DataHub is capable of performing aggregated transactional operations
// on all DataConn objects created from its registered DataSrc instances.
// The Run method executes logic without transaction control, while the Txn
// method executes logic within a controlled transaction.
type DataHub interface {
	DataAcc

	// Registers a session-local data source with this DataHub instance.
	//
	// This method is similar to the global Uses function but registers a data source
	// that is local to this specific DataHub session. Once the DataHub's state is
	// "fixed" (while Run function or Txn function is executing), further calls
	// to Uses are ignored. However, after Run or Txn completes, the DataHub's
	// "fixed" state is reset, allowing for new data sources to be registered or removed
	// via Disuses method in subsequent operations.
	Uses(name string, ds DataSrc)

	// Unregisters a session-local data source by its name.
	//
	// This method removes a data source that was previously registered via DataHub#Uses.
	// This operation is ignored if the DataHub's state is already "fixed".
	Disuses(name string)

	// Closes all session-local data sources registered in this DataHub instance.
	Close()

	begin() errs.Err
	commit() errs.Err
	rollback()
	postCommit()
	end()
}

type dataHubImpl struct {
	DataHub

	localDataSrcList dataSrcList
	dataSrcMap       map[string]*dataSrcContainer
	dataConnList     dataConnList
	dataConnMap      map[string]*dataConnContainer
	fixed            bool
}

// Creates a new DataHub instance.
//
// Upon creation, it attempts to "fix" the global data sources (making them immutable
// for further registration) and copies references to already set-up global data
// sources into its internal map for quick access.
func NewDataHub() DataHub {
	globalDataSrcsFixed = true

	dsMap := make(map[string]*dataSrcContainer)
	globalDataSrcList.copyContainerPtrsDidSetupInto(dsMap)

	return &dataHubImpl{
		localDataSrcList: dataSrcList{local: true},
		dataSrcMap:       dsMap,
		dataConnList:     dataConnList{},
		dataConnMap:      make(map[string]*dataConnContainer),
		fixed:            false,
	}
}

func (hub *dataHubImpl) Uses(name string, ds DataSrc) {
	if hub.fixed {
		return
	}

	hub.localDataSrcList.addDataSrc(name, ds)
}

func (hub *dataHubImpl) Disuses(name string) {
	if hub.fixed {
		return
	}

	maps.DeleteFunc(hub.dataSrcMap, func(nm string, ptr *dataSrcContainer) bool {
		return ptr.local && nm == name
	})
	hub.localDataSrcList.removeAndCloseContainerPtrDidSetupByName(name)
	hub.localDataSrcList.removeAndCloseContainerPtrNotSetupByName(name)
}

func (hub *dataHubImpl) Close() {
	clear(hub.dataConnMap)
	hub.dataConnList.closeDataConns()
	clear(hub.dataSrcMap)
	hub.localDataSrcList.closeDataSrcs()
}

func (hub *dataHubImpl) begin() errs.Err {
	hub.fixed = true

	errMap := hub.localDataSrcList.setupDataSrcs()
	hub.localDataSrcList.copyContainerPtrsDidSetupInto(hub.dataSrcMap)

	if len(errMap) > 0 {
		return errs.New(FailToSetupLocalDataSrcs{Errors: errMap})
	}

	return errs.Ok()
}

func (hub *dataHubImpl) commit() errs.Err {
	errMap := make(map[string]errs.Err)

	ag := AsyncGroup{}
	ptr := hub.dataConnList.head
	for ptr != nil {
		ag.name = ptr.name
		if err := ptr.conn.PreCommit(&ag); err.IsNotOk() {
			errMap[ptr.name] = err
			break
		}
		ptr = ptr.next
	}
	ag.joinAndPutErrorsInto(errMap)

	if len(errMap) > 0 {
		return errs.New(FailToPreCommitDataConn{Errors: errMap})
	}

	ag = AsyncGroup{}
	ptr = hub.dataConnList.head
	for ptr != nil {
		ag.name = ptr.name
		if err := ptr.conn.Commit(&ag); err.IsNotOk() {
			errMap[ptr.name] = err
			break
		}
		ptr = ptr.next
	}
	ag.joinAndPutErrorsInto(errMap)

	if len(errMap) > 0 {
		return errs.New(FailToCommitDataConn{Errors: errMap})
	}

	ag = AsyncGroup{}
	ptr = hub.dataConnList.head
	for ptr != nil {
		ag.name = ptr.name
		ptr.conn.PostCommit(&ag)
		ptr = ptr.next
	}
	ag.joinAndIgnoreErrors()

	return errs.Ok()
}

func (hub *dataHubImpl) rollback() {
	ag := AsyncGroup{}
	ptr := hub.dataConnList.head
	for ptr != nil {
		ag.name = ptr.name
		if ptr.conn.ShouldForceBack() {
			ptr.conn.ForceBack(&ag)
		} else {
			ptr.conn.Rollback(&ag)
		}
		ptr = ptr.next
	}

	ag.joinAndIgnoreErrors()
}

func (hub *dataHubImpl) end() {
	clear(hub.dataConnMap)
	hub.dataConnList.closeDataConns()
	hub.fixed = false
}

func (hub *dataHubImpl) getDataConn(name string, dataConnType string) (DataConn, errs.Err) {
	connPtr, ok := hub.dataConnMap[name]
	if ok {
		return connPtr.conn, errs.Ok()
	}

	dsPtr, ok := hub.dataSrcMap[name]
	if !ok {
		return nil, errs.New(NoDataSrcToCreateDataConn{Name: name, DataConnType: dataConnType})
	}

	conn, err := dsPtr.ds.CreateDataConn()
	if err.IsNotOk() {
		return nil, errs.New(FailToCreateDataConn{Name: name, DataConnType: dataConnType}, err)
	}
	if conn == nil {
		return nil, errs.New(CreatedDataConnIsNil{Name: name, DataConnType: dataConnType})
	}

	connPtr = &dataConnContainer{name: name, conn: conn}
	hub.dataConnMap[name] = connPtr
	hub.dataConnList.appendContainer(connPtr)

	return conn, errs.Ok()
}

// Retrieves a mutable reference to a DataConn object by name, creating it if necessary.
//
// This is the core method used by DataAcc implementations to obtain connections
// to external data services. It first checks if a DataConn with the given name
// already exists in the DataHub's session. If not, it attempts to find a
// corresponding DataSrc and create a new DataConn from it.
func GetDataConn[C DataConn](data any, name string) (C, errs.Err) {
	hub := data.(DataAcc)

	toType := typeNameOfTypeParam[C]()

	conn, err := hub.getDataConn(name, toType)
	if err.IsNotOk() {
		return *new(C), err
	}

	c, ok := conn.(C)
	if !ok {
		return *new(C), errs.New(FailToCastDataConn{Name: name, CastToType: toType})
	}
	return c, errs.Ok()
}

// Executes a given logic function without transaction control.
//
// This method sets up local data sources, runs the provided closure,
// and then cleans up the DataHub's session resources. It does not
// perform commit or rollback operations.
func Run[D any](hub DataHub, logic func(D) errs.Err) errs.Err {
	data, ok := hub.(D)
	if !ok {
		fromType := typeNameOf(&hub)[1:]
		toType := typeNameOfTypeParam[D]()
		return errs.New(FailToCastDataHub{CastFromType: fromType, CastToType: toType})
	}

	err := hub.begin()
	if err.IsNotOk() {
		return err
	}
	defer hub.end()

	err = logic(data)
	if err.IsNotOk() {
		return err
	}

	return errs.Ok()
}

// Executes a given logic function within a transaction.
//
// This method first sets up local data sources, then runs the provided closure.
// If the closure returns errs.Ok, it attempts to commit all changes. If the commit fails,
// or if the logic function itself returns an errs.Err, a rollback operation
// is performed. After succeeding PreCommit and Commit methods of all DataConn(s),
// PostCommit methods of all DataConn(s) are executed.
// Finally, it cleans up the DataHub's session resources.
func Txn[D any](hub DataHub, logic func(D) errs.Err) errs.Err {
	data, ok := hub.(D)
	if !ok {
		fromType := typeNameOf(&hub)[1:]
		toType := typeNameOfTypeParam[D]()
		return errs.New(FailToCastDataHub{CastFromType: fromType, CastToType: toType})
	}

	err := hub.begin()
	if err.IsNotOk() {
		return err
	}
	defer hub.end()

	err = logic(data)
	if err.IsOk() {
		err = hub.commit()
	}

	if err.IsNotOk() {
		hub.rollback()
		return err
	}

	return errs.Ok()
}
