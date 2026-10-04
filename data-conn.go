// Copyright (C) 2023-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"github.com/sttk/errs"
)

type /* error reasons */ (
	FailToPreCommitDataConn struct {
		Errors []ErrEntry
	}

	FailToCommitDataConn struct {
		Errors []ErrEntry
	}

	FailToPostCommitDataConn struct {
		Errors []ErrEntry
	}
)

type DataConn interface {
	PreCommit(ag *AsyncGroup) errs.Err
	Commit(ag *AsyncGroup) errs.Err
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
	list      []dataConnContainer
	indexMap  map[string]int
	committed bool
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
		if mgr.list[i].conn == nil {
			continue
		}
		ag._name = mgr.list[i].name
		ag._index = ii
		ii++
		if err := mgr.list[i].conn.PreCommit(&ag); err.IsNotOk() {
			ag.addErr(ag._index, ag._name, err)
			break
		}
	}
	errors := ag.join()

	if len(errors) > 0 {
		for i := range errors {
			idx := errors[i].Index
			reports[idx].Cause = TxnFailureCause{State: LogicFailure, Err: errors[i].Err}
		}
		return errs.New(FailToPreCommitDataConn{Errors: errors})
	}

	ag = AsyncGroup{}
	ii = 0
	for i := range mgr.list {
		if mgr.list[i].conn == nil {
			continue
		}
		ag._name = mgr.list[i].name
		ag._index = ii
		ii++
		if !mgr.list[i].conn.IsCommitted() {
			if err := mgr.list[i].conn.Commit(&ag); err.IsNotOk() {
				ag.addErr(ag._index, ag._name, err)
				break
			}
		}
	}
	errors = ag.join()

	if len(errors) > 0 {
		for i := range errors {
			idx := errors[i].Index
			reports[idx].Cause = TxnFailureCause{State: CommitFailure, Err: errors[i].Err}
		}
		return errs.New(FailToCommitDataConn{Errors: errors})
	}

	mgr.committed = true

	ag = AsyncGroup{}
	ii = 0
	for i := range mgr.list {
		if mgr.list[i].conn == nil {
			continue
		}
		ag._name = mgr.list[i].name
		ag._index = ii
		ii++
		if err := mgr.list[i].conn.PostCommit(&ag); err.IsNotOk() {
			ag.addErr(ag._index, ag._name, err)
			// don't break
		}
	}
	errors = ag.join()

	if len(errors) > 0 {
		for i := range errors {
			idx := errors[i].Index
			reports[idx].Cause = TxnFailureCause{State: PostCommitFailure, Err: errors[i].Err}
		}
		return errs.New(FailToPostCommitDataConn{Errors: errors})
	}

	return errs.Ok()
}

func (mgr *dataConnManager) rollback(reports []TxnFailureReport) {
	ag := AsyncGroup{}
	ii := 0
	for i := range mgr.list {
		if mgr.list[i].conn == nil {
			continue
		}
		ag._name = mgr.list[i].name
		ag._index = ii
		ii++
		if mgr.list[i].conn.IsCommitted() {
			if reports[ag._index].Cause.State == NoneByUncommitted {
				reports[ag._index].Cause.State = NoneByCommitted
			}
			continue
		}
		if mgr.committed {
			continue
		}
		if err := mgr.list[i].conn.Rollback(&ag); err.IsNotOk() {
			ag.addErr(ag._index, ag._name, err)
		} else {
			reports[ag._index].Rollback.State = NoneByRolledBack
		}
	}
	errors := ag.join()

	if len(errors) > 0 {
		for i := range errors {
			idx := errors[i].Index
			reports[idx].Rollback = TxnFailureRollback{State: RollbackFailure, Err: errors[i].Err}
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
	mgr.list = nil
}
