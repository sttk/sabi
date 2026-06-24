// Copyright (C) 2023-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"github.com/sttk/errs"
)

type /* error reasons */ (
	// FailToPreCommitDataConn represents an error reason indicating that one or more
	// data connections failed during their pre-commit phase. It wraps a list of
	// individual connection errors that occurred, allowing the caller to inspect
	// which connections encountered issues and what the underlying errors were before
	// the actual commit was attempted.
	FailToPreCommitDataConn struct {
		Errors []DataConnErr
	}

	// FailToCommitDataConn represents an error reason indicating that one or more
	// data connections failed during their commit phase. It contains a list of
	// errors from the failed connections, which is useful for diagnosing failures that
	// happened while finalizing the transactions on those connections.
	FailToCommitDataConn struct {
		Errors []DataConnErr
	}

	// FailToPostCommitDataConn represents an error reason indicating that one or more
	// data connections failed during their post-commit phase. This phase runs after
	// a successful commit, so this error implies that while the transaction was committed
	// successfully, subsequent cleanup or follow-up operations on the connections failed.
	// It contains a list of individual connection errors for diagnosis.
	FailToPostCommitDataConn struct {
		Errors []DataConnErr
	}
)

// DataConnErr represents a pair of a data connection's name and the error it encountered.
// It is used to report failures associated with specific connections during transaction
// phases such as pre-commit, commit, post-commit, or rollback.
type DataConnErr struct {
	// Name is the identifier of the data connection that failed.
	Name string
	// Err is the error returned by the data connection.
	Err errs.Err
}

// DataConn is an interface representing a database or external resource connection
// that participates in transaction management. It defines methods for managing the
// lifecycle of a transaction (pre-commit, commit, post-commit, rollback) as well as
// handling transaction failures and resource cleanup.
type DataConn interface {
	// Commit finalizes the changes made during the transaction. It receives an
	// AsyncGroup pointer to allow asynchronous execution of the commit operation.
	// It returns an error if the commit fails.
	Commit(ag *AsyncGroup) errs.Err

	// PreCommit performs preparation tasks before the actual commit is executed.
	// It receives an AsyncGroup pointer for asynchronous execution and returns
	// an error if the preparation fails.
	PreCommit(ag *AsyncGroup) errs.Err

	// PostCommit executes operations after the commit has completed successfully.
	// It receives an AsyncGroup pointer for asynchronous execution and returns
	// an error if post-commit operations fail.
	PostCommit(ag *AsyncGroup) errs.Err

	// IsCommitted returns a boolean indicating whether the transaction on this
	// connection has been successfully committed.
	IsCommitted() bool

	// Rollback aborts the changes made during the transaction, restoring the database
	// or resource to its state before the transaction began. It receives an
	// AsyncGroup pointer to allow asynchronous execution and returns an error
	// if the rollback operation fails.
	Rollback(ag *AsyncGroup) errs.Err

	// OnTxnFailure is called when the transaction fails. It allows the connection
	// to perform custom error handling, notifications, or diagnostics. It receives
	// an AsyncGroup pointer for asynchronous execution and a slice of reports
	// detailing the failures of all participating connections.
	OnTxnFailure(ag *AsyncGroup, reports []TxnFailureReport)

	// Close releases any resources associated with the connection. It is called
	// during cleanup after the transaction has concluded.
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
