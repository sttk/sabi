// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

package sabi

import (
	"github.com/sttk/errs"
)

type TxnFailureCause struct {
	State TxnFailureCauseState
	Err   errs.Err
}

type TxnFailureCauseState uint

const (
	NoneByCommitted TxnFailureCauseState = iota
	NoneByUncommitted
	LogicFailure
	CommitFailure
	PostCommitFailure
)

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

type TxnFailureRollback struct {
	State TxnFailureRollbackState
	Err   errs.Err
}

type TxnFailureRollbackState uint

const (
	NoneByRolledBack TxnFailureRollbackState = iota + 10
	NoneByNotRolledBack
	RollbackFailure
)

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

type TxnFailureRecovery uint

const (
	NoActionRequired TxnFailureRecovery = iota + 20
	RerunLogicAndCommit
	ResolveCauseThenRerunLogicAndCommit
	ResolveCauseThenRerunPostCommit
	ResolveCauseAndInconsistency
	InvestigateBecauseImpossible
	ManualRollbackRequired
)

type TxnFailureReport struct {
	DataConnName string
	DataConnType string
	Cause        TxnFailureCause
	Rollback     TxnFailureRollback
}

func newTxnFailureReport(name, typ string) TxnFailureReport {
	return TxnFailureReport{
		DataConnName: name,
		DataConnType: typ,
		Cause:        TxnFailureCause{State: NoneByUncommitted, Err: errs.Ok()},
		Rollback:     TxnFailureRollback{State: NoneByNotRolledBack, Err: errs.Ok()},
	}
}

func (rep *TxnFailureReport) IsCauseOfFailure() bool {
	switch rep.Cause.State {
	case NoneByCommitted, NoneByUncommitted:
		return false
	default:
		return true
	}
}

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
