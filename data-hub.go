// Copyright (C) 2023-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"maps"

	"github.com/sttk/errs"
)

type /* error reasons */ (
	FailToSetupGlobalDataSrcs struct {
		Errors map[string]errs.Err
	}

	FailToSetupLocalDataSrcs struct {
		Errors map[string]errs.Err
	}

	FailToCommitDataConn struct {
		Errors map[string]errs.Err
	}

	FailToPreCommitDataConn struct {
		Errors map[string]errs.Err
	}

	NoDataSrcToCreateDataConn struct {
		Name         string
		DataConnType string
	}

	FailToCreateDataConn struct {
		Name         string
		DataConnType string
	}

	CreatedDataConnIsNil struct {
		Name         string
		DataConnType string
	}

	FailToCastDataConn struct {
		Name       string
		CastToType string
	}

	FailToCastDataHub struct {
		CastFromType string
		CastToType   string
	}
)

var (
	globalDataSrcList   dataSrcList = dataSrcList{local: false}
	globalDataSrcsFixed bool        = false
)

func Uses(name string, ds DataSrc) {
	if !globalDataSrcsFixed {
		globalDataSrcList.addDataSrc(name, ds)
	}
}

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

func Close() {
	globalDataSrcList.closeDataSrcs()
}

type DataHub interface {
	DataAcc

	Uses(name string, ds DataSrc)
	Disuses(name string)
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
	hub.localDataSrcList.removeAndCloseLocalContainerPtrDidSetupByName(name)
	hub.localDataSrcList.removeAndCloseLocalContainerPtrNotSetupByName(name)
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

func (hub *dataHubImpl) postCommit() {
	ag := AsyncGroup{}
	ptr := hub.dataConnList.head
	for ptr != nil {
		ag.name = ptr.name
		ptr.conn.PostCommit(&ag)
		ptr = ptr.next
	}

	ag.joinAndIgnoreErrors()
}

func (hub *dataHubImpl) end() {
	clear(hub.dataConnMap)
	hub.dataConnList.closeDataConns()
	hub.fixed = false
}

const (
	no_error = iota
	no_data_src_to_create_data_conn
	fail_to_create_data_conn
	created_data_conn_is_nil
)

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

	hub.postCommit()
	return errs.Ok()
}
