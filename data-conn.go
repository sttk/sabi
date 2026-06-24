// Copyright (C) 2023-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"github.com/sttk/errs"
)

type /* error reasons */ (
	FailToPreCommitDataConn struct {
		Errors []DataConnErr
	}

	FailToCommitDataConn struct {
		Errors []DataConnErr
	}

	FailToPostCommitDataConn struct {
		Errors []DataConnErr
	}
)

type DataConnErr struct {
	Name string
	Err  errs.Err
}

type DataConn interface {
	Commit(ag *AsyncGroup) errs.Err
	PreCommit(ag *AsyncGroup) errs.Err
	PostCommit(ag *AsyncGroup) errs.Err
	IsCommitted() bool
	Rollback(ag *AsyncGroup) errs.Err
	OnTxnFailure(ag *AsyncGroup, reports []TxnFailureReport)
	Close()
}

type dataConnContainer struct {
	name string
	conn DataConn
}

type dataConnManager struct {
	list     []dataConnContainer
	indexMap map[string]int
}

func newDataConnManager() dataConnManager {
	return dataConnManager{
		list:     make([]dataConnContainer, 0),
		indexMap: make(map[string]int, 0),
	}
}

func newDataConnManagerWithCommitOrder(names []string) dataConnManager {
	mgr := dataConnManager{
		list:     make([]dataConnContainer, len(names)),
		indexMap: make(map[string]int, len(names)),
	}
	// Because earlier ones take precedence when names overlap
	for i := len(names) - 1; i >= 0; i-- {
		mgr.indexMap[names[i]] = i
		mgr.list[i].name = names[i]
	}
	return mgr
}

func (mgr *dataConnManager) add(cont dataConnContainer) {
	if idx, ok := mgr.indexMap[cont.name]; ok {
		// Because earlier ones take precedence when names overlap
		if mgr.list[idx].conn == nil {
			mgr.list[idx] = cont
		}
	} else {
		mgr.indexMap[cont.name] = len(mgr.list)
		mgr.list = append(mgr.list, cont)
	}
}

func (mgr *dataConnManager) findByName(name string) (*dataConnContainer, bool) {
	if idx, ok := mgr.indexMap[name]; ok {
		return &mgr.list[idx], true
	}
	return nil, false
}

func (mgr *dataConnManager) commitOrRollback(err errs.Err) errs.Err {
	reports := mgr.newFailureReports()
	if err.IsOk() {
		err = mgr.commit(reports)
	}
	if err.IsNotOk() {
		mgr.rollback(reports)
	}
	return err
}

func (mgr *dataConnManager) newFailureReports() []TxnFailureReport {
	reps := make([]TxnFailureReport, 0, len(mgr.indexMap))
	for i := range mgr.list {
		if mgr.list[i].conn != nil {
			reps = append(reps, newTxnFailureReport(mgr.list[i].name, typeNameOf(mgr.list[i].conn)))
		}
	}
	return reps
}

func (mgr *dataConnManager) commit(reports []TxnFailureReport) errs.Err {
	ag := AsyncGroup{}
	ii := 0
	for i := range mgr.list {
		if mgr.list[i].conn != nil {
			ag._index = ii
			if err := mgr.list[i].conn.PreCommit(&ag); err.IsNotOk() {
				ag.addErr(ag._index, err)
				break
			}
			ii++
		}
	}
	indexed_errors := ag.join()

	if len(indexed_errors) > 0 {
		errors := make([]DataConnErr, len(indexed_errors))
		for i := range indexed_errors {
			idx := indexed_errors[i].Index
			reports[idx].Cause = TxnFailureCause{State: LogicFailure, Err: indexed_errors[i].Err}
			errors[i].Name = reports[idx].DataConnName
			errors[i].Err = indexed_errors[i].Err
		}
		return errs.New(FailToPreCommitDataConn{Errors: errors})
	}

	ag = AsyncGroup{}
	ii = 0
	for i := range mgr.list {
		if mgr.list[i].conn != nil {
			if !mgr.list[i].conn.IsCommitted() {
				ag._index = ii
				if err := mgr.list[i].conn.Commit(&ag); err.IsNotOk() {
					ag.addErr(ag._index, err)
					break
				}
			}
			ii++
		}
	}
	indexed_errors = ag.join()

	if len(indexed_errors) > 0 {
		errors := make([]DataConnErr, len(indexed_errors))
		for i := range indexed_errors {
			idx := indexed_errors[i].Index
			reports[idx].Cause = TxnFailureCause{State: CommitFailure, Err: indexed_errors[i].Err}
			errors[i].Name = reports[idx].DataConnName
			errors[i].Err = indexed_errors[i].Err
		}
		return errs.New(FailToCommitDataConn{Errors: errors})
	}

	ag = AsyncGroup{}
	ii = 0
	for i := range mgr.list {
		if mgr.list[i].conn != nil {
			ag._index = ii
			if err := mgr.list[i].conn.PostCommit(&ag); err.IsNotOk() {
				ag.addErr(ag._index, err)
				// don't break
			}
			ii++
		}
	}
	indexed_errors = ag.join()

	if len(indexed_errors) > 0 {
		errors := make([]DataConnErr, len(indexed_errors))
		for i := range indexed_errors {
			idx := indexed_errors[i].Index
			reports[idx].Cause = TxnFailureCause{State: PostCommitFailure, Err: indexed_errors[i].Err}
			errors[i].Name = reports[idx].DataConnName
			errors[i].Err = indexed_errors[i].Err
		}
		return errs.New(FailToPostCommitDataConn{Errors: errors})
	}

	return errs.Ok()
}

func (mgr *dataConnManager) rollback(reports []TxnFailureReport) {
	ag := AsyncGroup{}
	ii := 0
	for i := range mgr.list {
		if mgr.list[i].conn != nil {
			if mgr.list[i].conn.IsCommitted() {
				if reports[ii].Cause.State == NoneByUncommitted {
					reports[ii].Cause.State = NoneByCommitted
				}
				continue
			}
			ag._index = ii
			if err := mgr.list[i].conn.Rollback(&ag); err.IsNotOk() {
				ag.addErr(ag._index, err)
			} else {
				reports[ii].Rollback.State = NoneByRolledBack
			}
			ii++
		}
	}
	indexed_errors := ag.join()

	if len(indexed_errors) > 0 {
		for i := range indexed_errors {
			idx := indexed_errors[i].Index
			reports[idx].Rollback = TxnFailureRollback{State: RollbackFailure, Err: indexed_errors[i].Err}
		}
	}

	ag = AsyncGroup{}
	for i := range mgr.list {
		if mgr.list[i].conn != nil {
			mgr.list[i].conn.OnTxnFailure(&ag, reports)
		}
	}
	_ = ag.join()
}

func (mgr *dataConnManager) close() {
	clear(mgr.indexMap)

	for i := len(mgr.list) - 1; i >= 0; i-- {
		if mgr.list[i].conn != nil {
			mgr.list[i].conn.Close()
		}
	}
	clear(mgr.list)
}
