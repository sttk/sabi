// Copyright (C) 2023-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"context"

	"github.com/sttk/errs"
)

type /* error reasons */ (
	FailToSetupGlobalDataSrcs struct {
		Errors []DataSrcErr
	}

	FailToSetupLocalDataSrcs struct {
		Errors []DataSrcErr
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
		Name           string
		ToDataConnType string
	}

	FailToCastDataHub struct {
		FromType string
		ToType   string
	}
)

var (
	globalDataSrcManager dataSrcManager = newDataSrcManager(false)
	globalDataSrcsFixed  bool           = false
)

func Uses(name string, ds DataSrc) {
	if !globalDataSrcsFixed {
		globalDataSrcManager.add(name, ds)
	}
}

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

func Shutdown() {
	globalDataSrcManager.close()
}

type DataHub interface {
	DataAcc

	Uses(name string, ds DataSrc)
	Disuses(name string)
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
