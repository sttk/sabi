// Copyright (C) 2023-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"github.com/sttk/errs"
)

type /* error reasons */ (
	FailToSetupLocalDataSrcs struct {
		Errors []ErrEntry
	}

	NoDataSrcToCreateDataConn struct {
		Name         string
		DataConnType string
	}

	CreatedDataConnIsNil struct {
		Name         string
		DataConnType string
	}

	FailToCreateDataConn struct {
		Name         string
		DataConnType string
	}

	CreatedDataConn struct {
		Name         string
		DataConnType string
	}

	FailToCastDataConn struct {
		Name             string
		FromDataConnType string
		ToDataConnType   string
	}

	FailToCastDataHub struct {
		Name     string
		FromType string
		ToType   string
	}
)

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

func (acc *DataAcc) Run[D any](logic func(D) errs.Err) errs.Err {
	data, ok := acc.dataHub.(D)
	if !ok {
		fromType := typeNameOf(acc.dataHub)
		toType := typeNameOfTypeParam[D]()
		return errs.New(FailToCastDataHub{FromType: fromType, ToType: toType})
	}

	return logic(data)
}

func (acc *DataAcc) Start() LogicRun {
	return newLogicRun(acc, true)
}
