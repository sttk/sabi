// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"github.com/sttk/errs"
)

// TxnFailureCause represents the specific cause of a transaction failure,
// combining a state indicator and the underlying error.
//
// It is a struct that encapsulates why a transaction failed, containing a state
// (TxnFailureCauseState) and the actual Go error (errs.Err).
//
// During a multi-resource transaction, failures can occur at various stages (e.g.,
// during business logic, pre-commit, commit, or post-commit). Identifying the exact
// stage and error is crucial for automated recovery decisions and developer debugging.
type TxnFailureCause struct {
	// State indicates the phase or condition of the transaction when the failure occurred.
	State TxnFailureCauseState
	// Err is the underlying error associated with the transaction failure cause.
	Err errs.Err
}

// TxnFailureCauseState represents the classification of transaction failure causes.
//
// It is an unsigned integer type used as an enum to define various transaction failure states.
//
// This type helps categorize exactly when or why a transaction failed, such as whether
// it failed during transaction logic execution or during the commit/post-commit phase.
type TxnFailureCauseState uint

// The following constants represent the specific states of a transaction failure cause.
const (
	// NoneByCommitted indicates that no failure occurred because the transaction was
	// successfully committed.
	NoneByCommitted TxnFailureCauseState = iota
	// NoneByUncommitted indicates that no failure occurred, but the transaction was
	// left uncommitted (e.g., explicitly rolled back before commit or failed earlier).
	NoneByUncommitted
	// LogicFailure indicates that the transaction failed during the execution of the
	// application's business logic.
	LogicFailure
	// CommitFailure indicates that the transaction failed during the commit phase of
	// one or more data connections.
	CommitFailure
	// PostCommitFailure indicates that the transaction failed during the post-commit
	// phase after a successful commit.
	PostCommitFailure
)

// String returns the string representation of the TxnFailureCauseState.
//
// It maps the TxnFailureCauseState enum value to its corresponding string literal name,
// which is useful for logging, debugging, or displaying state information.
func (state TxnFailureCauseState) String() string {
	var s string
	switch state {
	case NoneByCommitted:
		s = "NoneByCommitted"
	case NoneByUncommitted:
		s = "NoneByUncommitted"
	case LogicFailure:
		s = "LogicFailure"
	case CommitFailure:
		s = "CommitFailure"
	case PostCommitFailure:
		s = "PostCommitFailure"
	}
	return s
}

// TxnFailureRollback represents the status and error of a rollback operation
// performed after a transaction failure.
//
// It is a struct containing a state (TxnFailureRollbackState) representing the outcome
// of a rollback attempt and the associated error (errs.Err) if the rollback failed.
//
// When a transaction fails, the system attempts to roll back active connections to maintain
// consistency. This struct tracks whether the rollback succeeded, was not attempted, or failed,
// helping to determine if the system is left in an inconsistent state.
type TxnFailureRollback struct {
	// State indicates the outcome of the rollback operation.
	State TxnFailureRollbackState
	// Err is the error returned by the rollback operation, if any.
	Err errs.Err
}

// TxnFailureRollbackState represents the classification of transaction rollback states.
//
// It is an unsigned integer type used as an enum to define the result of a rollback attempt.
//
// It categorizes the rollback status (e.g., rolled back successfully, not rolled back, or
// failed to roll back) to assist in determining the appropriate recovery path.
type TxnFailureRollbackState uint

// The following constants represent the specific states of a transaction rollback.
const (
	// NoneByRolledBack indicates that the rollback was successfully executed.
	NoneByRolledBack TxnFailureRollbackState = iota + 10
	// NoneByNotRolledBack indicates that no rollback was executed (e.g., because the
	// transaction was already committed or rollback was not needed).
	NoneByNotRolledBack
	// RollbackFailure indicates that a rollback was attempted but failed, potentially
	// leaving the data source in an inconsistent state.
	RollbackFailure
)

// String returns the string representation of the TxnFailureRollbackState.
//
// It maps the TxnFailureRollbackState enum value to its corresponding string literal name.
func (state TxnFailureRollbackState) String() string {
	var s string
	switch state {
	case NoneByRolledBack:
		s = "NoneByRolledBack"
	case NoneByNotRolledBack:
		s = "NoneByNotRolledBack"
	case RollbackFailure:
		s = "RollbackFailure"
	}
	return s
}

// TxnFailureRecovery represents the recommended action or strategy to recover from
// a transaction failure.
//
// It is an unsigned integer type used as an enum to guide automated or manual recovery paths.
//
// By analyzing both the cause of a transaction failure and the status of the rollback, the system
// recommends a recovery strategy to restore consistency or safely retry the transaction.
type TxnFailureRecovery uint

// The following constants define the various recovery actions recommended for a transaction
// failure.
const (
	// NoActionRequired indicates that no recovery action is needed because the operation
	// completed successfully or was safely rolled back.
	NoActionRequired TxnFailureRecovery = iota + 20
	// RerunLogicAndCommit indicates that the transaction can be safely retried from the
	// beginning (rerunning the business logic and attempting commit again).
	RerunLogicAndCommit
	// ResolveCauseThenRerunLogicAndCommit indicates that the external cause of the failure
	// (e.g., network issue, constraint violation) must be resolved first, and then the
	// transaction can be retried from the beginning.
	ResolveCauseThenRerunLogicAndCommit
	// ResolveCauseThenRerunPostCommit indicates that the transaction committed successfully,
	// but a post-commit task failed. The underlying cause should be resolved, and only the
	// post-commit logic should be re-executed.
	ResolveCauseThenRerunPostCommit
	// ResolveCauseAndInconsistency indicates that both the cause of the failure and the resulting
	// data inconsistency (due to a rollback failure) must be resolved before proceeding.
	ResolveCauseAndInconsistency
	// InvestigateBecauseImpossible indicates an unexpected or logically impossible state transition
	// has occurred, requiring manual developer investigation.
	InvestigateBecauseImpossible
	// ManualRollbackRequired indicates that the transaction succeeded/committed on some resources
	// but failed on others, requiring manual intervention to roll back or reconcile the committed data.
	ManualRollbackRequired
)

// TxnFailureReport aggregates details about a transaction failure for a specific
// data connection.
//
// It is a struct that records the connection name, connection type, the failure cause,
// and the rollback status of a single transaction connection.
//
// When managing distributed transactions or transactions over multiple data sources, a failure
// report is compiled for each data connection. This aggregated report enables overall transaction
// coordinators to decide how to recover the system.
type TxnFailureReport struct {
	// DataConnName is the name identifying the specific data connection.
	DataConnName string
	// DataConnType is the type name of the data connection.
	DataConnType string
	// Cause describes what caused the transaction to fail on this connection.
	Cause TxnFailureCause
	// Rollback describes the result of the rollback attempt on this connection.
	Rollback TxnFailureRollback
}

func newTxnFailureReport(name, typ string) TxnFailureReport {
	return TxnFailureReport{
		DataConnName: name,
		DataConnType: typ,
		Cause:        TxnFailureCause{State: NoneByUncommitted, Err: errs.Ok()},
		Rollback:     TxnFailureRollback{State: NoneByNotRolledBack, Err: errs.Ok()},
	}
}

// IsCauseOfFailure checks if this report indicates a connection failure.
//
// It evaluates the internal Cause.State to determine whether this specific connection was
// a source of transaction failure (i.e. if the cause state is not NoneByCommitted and not
// NoneByUncommitted).
//
// Returns true if the connection experienced an actual failure (e.g., LogicFailure,
// CommitFailure, PostCommitFailure); returns false otherwise.
func (rep *TxnFailureReport) IsCauseOfFailure() bool {
	switch rep.Cause.State {
	case NoneByCommitted, NoneByUncommitted:
		return false
	default:
		return true
	}
}

// RecoveryForCommit determines the recovery strategy when a transaction fails during a commit
// attempt.
//
// It analyzes the combination of the transaction failure cause (rep.Cause.State) and the
// rollback outcome (rep.Rollback.State) to recommend an appropriate recovery action.
//
// When a commit fails, this method helps the application decide whether it can safely retry the
// transaction, needs to resolve errors first, or requires developer investigation due to data
// inconsistency or logic errors.
//
// This function returns a TxnFailureRecovery value representing the recommended strategy.
func (rep *TxnFailureReport) RecoveryForCommit() TxnFailureRecovery {
	switch rep.Cause.State {
	case NoneByUncommitted:
		switch rep.Rollback.State {
		case NoneByNotRolledBack:
			return InvestigateBecauseImpossible
		case NoneByRolledBack:
			return RerunLogicAndCommit
		case RollbackFailure:
			return ResolveCauseAndInconsistency
		default:
			return InvestigateBecauseImpossible
		}
	case NoneByCommitted:
		switch rep.Rollback.State {
		case NoneByNotRolledBack:
			return NoActionRequired
		default:
			return InvestigateBecauseImpossible
		}
	case LogicFailure:
		switch rep.Rollback.State {
		case NoneByNotRolledBack:
			return InvestigateBecauseImpossible
		case NoneByRolledBack:
			return ResolveCauseThenRerunLogicAndCommit
		case RollbackFailure:
			return ResolveCauseAndInconsistency
		default:
			return InvestigateBecauseImpossible
		}
	case CommitFailure:
		switch rep.Rollback.State {
		case NoneByNotRolledBack:
			return InvestigateBecauseImpossible
		case NoneByRolledBack:
			return ResolveCauseThenRerunLogicAndCommit
		case RollbackFailure:
			return ResolveCauseAndInconsistency
		default:
			return InvestigateBecauseImpossible
		}
	case PostCommitFailure:
		switch rep.Rollback.State {
		case NoneByNotRolledBack:
			return ResolveCauseThenRerunPostCommit
		default:
			return InvestigateBecauseImpossible
		}
	default:
		return InvestigateBecauseImpossible
	}
}

// RecoveryForRollback determines the recovery strategy when a transaction is explicitly rolled
// back.
//
// What it does:
// It analyzes the transaction failure cause and the rollback outcome to recommend a recovery
// action,
// specifically tailored for rollback scenarios.
//
// When a transaction is intentionally aborted or a rollback is triggered, this method evaluates the
// success of the rollback to determine if further reconciliation or manual database intervention is
// required.
//
// This function returns a TxnFailureRecovery value representing the recommended strategy.
func (rep *TxnFailureReport) RecoveryForRollback() TxnFailureRecovery {
	switch rep.Cause.State {
	case NoneByUncommitted:
		switch rep.Rollback.State {
		case NoneByNotRolledBack:
			return InvestigateBecauseImpossible
		case NoneByRolledBack:
			return NoActionRequired
		case RollbackFailure:
			return ResolveCauseAndInconsistency
		default:
			return InvestigateBecauseImpossible
		}
	case NoneByCommitted:
		switch rep.Rollback.State {
		case NoneByNotRolledBack:
			return ManualRollbackRequired
		default:
			return InvestigateBecauseImpossible
		}
	case LogicFailure:
		switch rep.Rollback.State {
		case NoneByNotRolledBack:
			return InvestigateBecauseImpossible
		case NoneByRolledBack:
			return NoActionRequired
		case RollbackFailure:
			return ResolveCauseAndInconsistency
		default:
			return InvestigateBecauseImpossible
		}
	case CommitFailure:
		switch rep.Rollback.State {
		case NoneByNotRolledBack:
			return InvestigateBecauseImpossible
		case NoneByRolledBack:
			return NoActionRequired
		case RollbackFailure:
			return ResolveCauseAndInconsistency
		default:
			return InvestigateBecauseImpossible
		}
	case PostCommitFailure:
		switch rep.Rollback.State {
		case NoneByNotRolledBack:
			return ManualRollbackRequired
		default:
			return InvestigateBecauseImpossible
		}
	default:
		return InvestigateBecauseImpossible
	}
}
